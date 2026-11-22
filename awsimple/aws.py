"""
AWS access: AWSAccess, the base class of every awsimple service class. It makes the boto3 session, client and resource (for AWS or the moto mock).
"""

import os
import sys
import threading
from dataclasses import dataclass, field
from typing import Union, Any
from logging import getLogger

from typeguard import typechecked

from boto3.session import Session
from botocore.credentials import Credentials

from awsimple import __application_name__, is_mock
from .exceptions import AWSimpleException  # noqa: F401  (re-exported for backwards compatibility)

log = getLogger(__application_name__)

# Process-wide moto mock state. Multiple AWSAccess instances share one moto mock and one saved copy of the AWS
# environment variables, so instance creation/deletion order can't corrupt the environment or stop a mock another
# instance still needs.
_moto_lock = threading.Lock()


@dataclass
class _MotoState:
    """
    The process-wide state of the moto mock (see _moto_acquire() and _moto_release()).
    """

    count: int = 0  # number of AWSAccess instances using the mock
    mock: Any = None  # the moto mock, while it is started
    saved_env: dict[str, str | None] = field(default_factory=dict)  # the AWS environment variables as they were before the mock (None if not set)


_moto_state = _MotoState()
_mock_env_keys = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SECURITY_TOKEN", "AWS_SESSION_TOKEN"]

# One boto3 client or resource is made at a time in the process. boto3 sessions are not thread safe (and with the moto mock every instance uses boto3's one default
# session): clients made from threads at once now and then fail (e.g. "ValueError: Invalid endpoint: https://{service}.{region}.{dnsSuffix}"), and without the GIL
# (free-threaded Python) the threads also slow each other down so much that making them one at a time is far faster. The clients and resources are thread safe once made.
_client_creation_lock = threading.Lock()

# AWS services that boto3 has a client for but no resource
services_without_resource = ("logs", "rds")


def _make_moto_s3_safe_for_threads():
    """
    Stop the moto mock of S3 from reloading its moto.s3.urls module on every request. A reload takes the module out of sys.modules for a moment, so S3 requests made from
    threads at once (e.g. the parts of a download, or S3Access.sync_to_directory()) now and then fail with "ImportError: module moto.s3.urls not in sys.modules". That is
    rare with the GIL and common without it (free-threaded Python). moto reloads the module to pick up changes to its environment variables, which do not change while
    the mock is in use.
    """
    import moto.s3.urls as moto_s3_urls
    from moto.s3.models import S3Backend

    setattr(S3Backend, "_url_module", property(lambda _unused_self: moto_s3_urls))


def _moto_acquire():
    """
    Start using the moto mock. The first use starts the mock and sets the AWS environment variables to test values (the real ones are saved).
    """
    with _moto_lock:
        if _moto_state.count == 0:
            for aws_key in _mock_env_keys:
                _moto_state.saved_env[aws_key] = os.environ.get(aws_key)  # will be None if not set
                os.environ[aws_key] = "testing"

            from moto import mock_aws

            _moto_state.mock = mock_aws()
            _moto_state.mock.start()
            _make_moto_s3_safe_for_threads()
        _moto_state.count += 1


def _moto_release():
    """
    Stop using the moto mock. When the last user stops, the mock is stopped and the AWS environment variables are restored.
    """
    with _moto_lock:
        if _moto_state.count > 0:
            _moto_state.count -= 1
            if _moto_state.count == 0:
                if _moto_state.mock is not None:
                    _moto_state.mock.stop()
                    _moto_state.mock = None
                for aws_key, value in _moto_state.saved_env.items():
                    if value is None:
                        os.environ.pop(aws_key, None)
                    else:
                        os.environ[aws_key] = value
                _moto_state.saved_env = {}


def boto_error_to_string(boto_error) -> Union[str, None]:
    """
    Get a short string for a boto3 error: the AWS error code if the error has one (e.g. "NoSuchKey"), otherwise the error as a string.

    :param boto_error: a botocore exception
    :return: error string (None if the error has a response with no error in it)
    """
    # BotoCoreError subclasses (e.g. HTTPClientError) don't have a .response attribute - only ClientError does
    if (response := getattr(boto_error, "response", None)) is None:
        most_recent_error = str(boto_error)
    else:
        if (response_error := response.get("Error")) is None:
            most_recent_error = None
        else:
            most_recent_error = response_error.get("Code")
    return most_recent_error


class AWSAccess:
    """
    Basic AWS access (session, client and resource), some basic AWS information, and mock support for testing. The base class of every awsimple service class.
    """

    @typechecked()
    def __init__(
        self,
        resource_name: Union[str, None] = None,
        profile_name: Union[str, None] = None,
        aws_access_key_id: Union[str, None] = None,
        aws_secret_access_key: Union[str, None] = None,
        region_name: Union[str, None] = None,
    ):
        """
        AWSAccess - takes care of basic AWS access (e.g. session, client, resource), getting some basic AWS information, and mock support for testing.

        :param resource_name: AWS resource name (e.g. s3, dynamodb, sqs, sns, etc.). Can be None if just testing the connection.

        # Provide either: profile name or access key ID/secret access key pair

        :param profile_name: AWS profile name
        :param aws_access_key_id: AWS access key (required if secret_access_key given)
        :param aws_secret_access_key: AWS secret access key (required if access_key_id given)
        :param region_name: AWS region (may be optional - see AWS docs)
        """

        import boto3  # import here to facilitate mocking

        self.resource_name = resource_name
        self.profile_name = profile_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        # string representation of AWS most recent error code
        self.most_recent_error = None  # type: Union[str, None]

        self._is_mocked = False

        # use keys in AWS config
        # https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
        kwargs = {}
        for k in ["profile_name", "aws_access_key_id", "aws_secret_access_key", "region_name"]:
            if getattr(self, k) is not None:
                kwargs[k] = getattr(self, k)
        self.session = boto3.session.Session(**kwargs)

        self.client: Any = None
        self.resource: Any = None
        if is_mock():
            self._create_mock_client_and_resource(boto3)
        else:
            self._create_aws_client_and_resource()

    def _create_client_and_resource(self, factory: Any, **kwargs) -> None:
        """
        Make the boto3 client, and the resource if the service has one, for this instance's service.

        :param factory: what makes them: the boto3 module (default session) or a boto3 session
        :param kwargs: keyword arguments for both, e.g. region_name or config
        """
        if self.resource_name is not None:  # None: just the session, but not the client or resource
            with _client_creation_lock:
                self.client = factory.client(self.resource_name, **kwargs)
                if self.resource_name not in services_without_resource:
                    self.resource = factory.resource(self.resource_name, **kwargs)

    def _create_session_client(self, service_name: str, **kwargs) -> Any:
        """
        Make a further boto3 client from this instance's session (so its profile, keys and region are used), one at a time in the process (see _client_creation_lock).

        :param service_name: AWS service name, e.g. s3 or sts
        :param kwargs: keyword arguments for the client, e.g. config
        :return: the boto3 client
        """
        with _client_creation_lock:
            session: Any = self.session  # the service name is not a literal here, which is what the boto3 type stubs are written for
            return session.client(service_name, **kwargs)

    def _create_mock_client_and_resource(self, boto3) -> None:
        """
        Make the client and resource for the moto mock.

        :param boto3: the boto3 module
        """
        _moto_acquire()
        self._is_mocked = True
        self._create_client_and_resource(boto3, region_name="us-east-1")
        if self.resource_name == "s3":
            assert self.resource is not None
            self.resource.create_bucket(Bucket="testawsimple")  # todo: put this in the test code

    def _create_aws_client_and_resource(self) -> None:
        """
        Make the client and resource for AWS, from this instance's session (so its profile, keys and region are used).
        """
        self._create_client_and_resource(self.session, config=self._get_config())

    def _get_config(self):
        """
        :return: the botocore configuration for AWS clients and resources (with timeouts long enough for slow connections)
        """
        from botocore.config import Config  # import here to facilitate mocking

        timeout = 60 * 60  # AWS default is 60, which is too short for some uses and/or connections
        return Config(connect_timeout=timeout, read_timeout=timeout)

    @typechecked()
    def get_region(self) -> Union[str, None]:
        """
        Get current selected AWS region

        :return: region string
        """
        return self.session.region_name

    def get_access_key(self) -> Union[str, None]:
        """
        Get current access key string

        :return: access key
        """
        _session = self.session
        assert isinstance(_session, Session)  # for mypy
        _credentials = _session.get_credentials()
        assert isinstance(_credentials, Credentials)  # for mypy
        access_key = _credentials.access_key
        return access_key

    def get_account_id(self):
        """
        Get AWS account ID *** HAS BEEN REMOVED ***

        :return: account ID
        """
        raise NotImplementedError(".get_account_id() has been removed")

    def test(self) -> bool:
        """
        Basic connection/credentials test. Calls STS GetCallerIdentity, which requires no special permissions but does require valid credentials.
        Returns False if the credentials are invalid. Configuration errors (e.g. botocore's ProfileNotFound) still raise.

        :return: True if connection OK, False if the credentials are invalid
        """
        from botocore.exceptions import ClientError, NoCredentialsError  # import here to facilitate mocking

        sts_client = self._create_session_client("sts")
        try:
            sts_client.get_caller_identity()  # raises if credentials are invalid
        except (ClientError, NoCredentialsError) as e:
            log.info(f"{self.profile_name=} {e}")
            self.most_recent_error = boto_error_to_string(e)
            return False
        return True  # if we got here, we were successful

    def is_mocked(self) -> bool:
        """
        Return True if currently mocking the AWS interface (e.g. for testing).

        :return: True if mocked
        """
        return self._is_mocked

    def clear_most_recent_error(self):
        """
        Clear the most recent AWS error (self.most_recent_error).
        """
        self.most_recent_error = None

    def __del__(self):
        """
        Stop using the moto mock, if this instance uses it.
        """
        # At interpreter shutdown the moto mock and the modules it uses may already be gone, so stopping it could fail in
        # any number of ways. Nothing needs to be released then: the process is ending.
        if getattr(self, "_is_mocked", False) and not sys.is_finalizing():
            self._is_mocked = False
            _moto_release()
