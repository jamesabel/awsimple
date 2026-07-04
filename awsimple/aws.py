import os
import threading
from typing import Union, Any
from logging import getLogger

from typeguard import typechecked

from boto3.session import Session
from botocore.credentials import Credentials

from awsimple import __application_name__, is_mock, is_using_localstack
from .exceptions import AWSimpleException  # noqa: F401  (re-exported for backwards compatibility)

log = getLogger(__application_name__)

# Process-wide moto mock state. Multiple AWSAccess instances share one moto mock and one saved copy of the AWS
# environment variables, so instance creation/deletion order can't corrupt the environment or stop a mock another
# instance still needs.
_moto_lock = threading.Lock()
_moto_state = {"count": 0, "mock": None, "saved_env": {}}  # type: dict
_mock_env_keys = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SECURITY_TOKEN", "AWS_SESSION_TOKEN"]


def _moto_acquire():
    with _moto_lock:
        if _moto_state["count"] == 0:
            for aws_key in _mock_env_keys:
                _moto_state["saved_env"][aws_key] = os.environ.get(aws_key)  # will be None if not set
                os.environ[aws_key] = "testing"

            from moto import mock_aws

            _moto_state["mock"] = mock_aws()
            _moto_state["mock"].start()
        _moto_state["count"] += 1


def _moto_release():
    with _moto_lock:
        if _moto_state["count"] > 0:
            _moto_state["count"] -= 1
            if _moto_state["count"] == 0:
                if _moto_state["mock"] is not None:
                    _moto_state["mock"].stop()
                    _moto_state["mock"] = None
                for aws_key, value in _moto_state["saved_env"].items():
                    if value is None:
                        os.environ.pop(aws_key, None)
                    else:
                        os.environ[aws_key] = value
                _moto_state["saved_env"] = {}


def boto_error_to_string(boto_error) -> Union[str, None]:
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

        self.client = None  # type: Any
        if is_mock():
            # moto mock AWS
            _moto_acquire()
            self._is_mocked = True
            region = "us-east-1"
            if self.resource_name == "logs" or self.resource_name is None:
                # logs don't have a resource
                self.resource = None
            else:
                self.resource = boto3.resource(self.resource_name, region_name=region)  # type: ignore
            if self.resource_name is None:
                self.client = None
            else:
                self.client = boto3.client(self.resource_name, region_name=region)  # type: ignore
            if self.resource_name == "s3":
                assert self.resource is not None
                self.resource.create_bucket(Bucket="testawsimple")  # todo: put this in the test code
        elif is_using_localstack():
            self.aws_access_key_id = "test"
            self.aws_secret_access_key = "test"
            self.region_name = "us-west-2"
            if self.resource_name is None:
                # just the session, but not the client or resource
                self.client = None
                self.resource = None
            else:
                if self.resource_name == "logs":
                    # logs don't have resource
                    self.resource = None
                else:
                    self.resource = boto3.resource(self.resource_name, endpoint_url=self._get_localstack_endpoint_url())  # type: ignore
                self.client = boto3.client(self.resource_name, endpoint_url=self._get_localstack_endpoint_url())  # type: ignore
        elif self.resource_name is None:
            # just the session, but not the client or resource
            self.client = None
            self.resource = None
        else:
            self.client = self.session.client(self.resource_name, config=self._get_config())  # type: ignore
            if self.resource_name == "logs" or self.resource_name == "rds":
                # logs and rds don't have a resource
                self.resource = None
            else:
                self.resource = self.session.resource(self.resource_name, config=self._get_config())  # type: ignore

    def _get_localstack_endpoint_url(self) -> str | None:
        endpoint_url = "http://localhost:4566"  # default localstack endpoint
        return endpoint_url

    def _get_config(self):
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
        Raises an exception (e.g. botocore's ClientError, NoCredentialsError, or ProfileNotFound) if the connection or credentials are bad.

        :return: True if connection OK
        """
        if is_using_localstack():
            sts_client = self.session.client("sts", endpoint_url=self._get_localstack_endpoint_url())
        else:
            sts_client = self.session.client("sts")
        sts_client.get_caller_identity()  # raises if credentials are invalid
        return True  # if we got here, we were successful

    def is_mocked(self) -> bool:
        """
        Return True if currently mocking the AWS interface (e.g. for testing).

        :return: True if mocked
        """
        return self._is_mocked

    def clear_most_recent_error(self):
        self.most_recent_error = None

    def __del__(self):
        if getattr(self, "_is_mocked", False):
            self._is_mocked = False
            try:
                _moto_release()
            except Exception:  # noqa: S110 - interpreter may be shutting down
                pass
