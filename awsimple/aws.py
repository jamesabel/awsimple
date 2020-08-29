import logging
from pathlib import Path
import math

import boto3
from botocore.config import Config
from appdirs import user_cache_dir

from awsimple import __application_name__, __author__

log = logging.getLogger(__application_name__)


class AWSAccess:
    def __init__(self, resource_name: str, profile_name: str = None, access_key_id: str = None, secret_access_key: str = None,
                 cache_dir: Path = None, cache_life: float = math.inf, cache_max_absolute: int = round(1e9)):
        self.resource_name = resource_name
        self.profile_name = profile_name
        self.access_key_id = access_key_id
        self.secret_access_key: secret_access_key

        if cache_dir is None:
            self.cache_dir = Path(user_cache_dir(__application_name__, __author__), "aws", resource_name)
        else:
            self.cache_dir = cache_dir

        self.cache_retries = 10
        self.cache_max_absolute = cache_max_absolute  # max absolute cache size
        self.cache_max_of_free = 0.05  # max portion of the disk's free space this LRU cache will take
        self.cache_life = cache_life  # seconds
        self.abs_tol = 10.0  # file modification times within this cache window (in seconds) are considered equivalent

        # use keys in AWS config
        # https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
        if self.profile_name is not None:
            self.session = boto3.session.Session(profile_name=self.profile_name)
        elif self.access_key_id is not None and self.secret_access_key is not None:
            self.session = boto3.session.Session(aws_access_key_id=self.access_key_id, aws_secret_access_key=self.secret_access_key)
        else:
            raise ValueError("AWS profile or access keys must be specified")

        self.client = self.session.client(self.resource_name, config=self._get_config())
        self.resource = self.session.resource(self.resource_name, config=self._get_config())

    def _get_config(self):
        timeout = 60 * 60  # AWS default is 60
        return Config(connect_timeout=timeout, read_timeout=timeout)

    def get_region(self) -> str:
        return self.session.region_name
