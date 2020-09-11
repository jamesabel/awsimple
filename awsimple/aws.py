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
    @typechecked(always=True)
    def __init__(
        self,
        resource_name: str = None,
        profile_name: str = None,
        access_key_id: str = None,
        secret_access_key: str = None,
        cache_dir: Path = None,
        cache_life: float = math.inf,
        cache_max_absolute: int = round(1e9),
        cache_max_of_free: float = 0.05,
        mtime_abs_tol: float = 10.0,
    ):
        self.resource_name = resource_name
        self.profile_name = profile_name
        self.access_key_id = access_key_id
        self.secret_access_key: secret_access_key

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
        if self.access_key_id is not None and self.secret_access_key is not None:
            self.session = boto3.session.Session(aws_access_key_id=self.access_key_id, aws_secret_access_key=self.secret_access_key)
        elif self.profile_name is not None:
            self.session = boto3.session.Session(profile_name=self.profile_name)
        else:
            self.session = boto3.session.Session()  # defaults

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

    @typechecked(always=True)
    def get_region(self) -> str:
        return self.session.region_name

    def get_access_key(self):
        return self.session.get_credentials().access_key

    def test(self):
        # basic connection/capability test
        resources = self.session.get_available_resources()  # boto3 will throw an error if there's an issue here
        if self.resource_name is not None and self.resource_name not in resources:
            raise PermissionError(self.resource_name)  # we don't have permission to the specified resource
