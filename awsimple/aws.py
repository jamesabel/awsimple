import logging
from pathlib import Path
import math

import boto3
from botocore.config import Config
from appdirs import user_cache_dir
from typeguard import typechecked

from awsimple import __application_name__, __author__

log = logging.getLogger(__application_name__)


class AWSimpleException(Exception):
    pass


class AWSAccess:
    @typechecked()
    def __init__(
        self,
        resource_name: str = None,
        profile_name: str = None,

        aws_access_key_id : str = None,
        aws_secret_access_key : str = None,

        region_name: str = None,

        cache_dir: Path = None,
        cache_life: float = math.inf,
        cache_max_absolute: int = round(1e9),
        cache_max_of_free: float = 0.05,
        mtime_abs_tol: float = 10.0,
    ):
        """
        AWS access
        :param resource_name: AWS resource name (e.g. s3, dynamodb, sqs, sns, etc.)

        # See AWS docs for use of profile name and/or access key ID/secret access key pair, as well as region name.
        :param profile_name: AWS profile name
        :param aws_access_key_id: AWS access key (required if secret_access_key given)
        :param aws_secret_access_key: AWS secret access key (required if access_key_id given)
        :param region_name: AWS region (may be optional - see AWS docs)

        :param cache_dir: dir for cache
        :param cache_life: life of cache (in seconds)
        :param cache_max_absolute: max size of cache
        :param cache_max_of_free: max portion of disk free space the cache will consume
        :param mtime_abs_tol: window in seconds where a modification time will be considered equal
        """
        self.resource_name = resource_name
        self.profile_name = profile_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        if cache_dir is None:
            if resource_name is None:
                self.cache_dir = Path(user_cache_dir(__application_name__, __author__), "aws")
            else:
                self.cache_dir = Path(user_cache_dir(__application_name__, __author__), "aws", resource_name)
        else:
            self.cache_dir = cache_dir

        self.cache_life = cache_life  # seconds
        self.cache_max_absolute = cache_max_absolute  # max absolute cache size
        self.cache_max_of_free = cache_max_of_free  # max portion of the disk's free space this LRU cache will take
        self.mtime_abs_tol = mtime_abs_tol  # file modification times within this cache window (in seconds) are considered equivalent
        self.cache_retries = 10  # cache upload retries

        # use keys in AWS config
        # https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
        kwargs = {}
        for k in ["profile_name", "aws_access_key_id", "aws_secret_access_key", "region_name"]:
            if getattr(self, k) is not None:
                kwargs[k] = getattr(self, k)
        self.session = boto3.session.Session(**kwargs)

        if self.resource_name is None:
            # just the session, but not the client or resource
            self.client = None
            self.resource = None
        else:
            self.client = self.session.client(self.resource_name, config=self._get_config())
            self.resource = self.session.resource(self.resource_name, config=self._get_config())

    def _get_config(self):
        timeout = 60 * 60  # AWS default is 60, which is too short for some uses and/or connections
        return Config(connect_timeout=timeout, read_timeout=timeout)

    @typechecked()
    def get_region(self) -> str:
        return self.session.region_name

    def get_access_key(self):
        return self.session.get_credentials().access_key

    def get_account_id(self):
        arn = self.session.resource("iam").CurrentUser().arn
        log.info("current user {arn=}")
        return arn.split(":")[4]

    def test(self) -> bool:
        # basic connection/capability test
        resources = self.session.get_available_resources()  # boto3 will throw an error if there's an issue here
        if self.resource_name is not None and self.resource_name not in resources:
            raise PermissionError(self.resource_name)  # we don't have permission to the specified resource
        return True  # if we got here, we were successful
