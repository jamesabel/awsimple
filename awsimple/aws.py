import logging
from dataclasses import dataclass
from pathlib import Path

import boto3
from botocore.config import Config

from awsimple import __application_name__

log = logging.getLogger(__application_name__)


@dataclass
class AWSAccess:
    profile_name: str = None
    access_key_id: str = None
    secret_access_key: str = None

    session = None
    resource = None
    client = None

    cache_dir: Path = None
    cache_retries: int = 10
    cache_max_absolute: int = round(1e9)  # max absolute cache size
    cache_max_of_free: float = 0.05  # max portion of the disk's free space this LRU cache will take
    cache_life: float = None  # seconds
    abs_tol: float = 10.0  # file modification times within this cache window (in seconds) are considered equivalent

    def _get_config(self):
        timeout = 60 * 60  # AWS default is 60
        return Config(connect_timeout=timeout, read_timeout=timeout)

    def _get_client_and_resource(self, resource_name: str):

        if self.client is None or self.resource is None:
            # use keys in AWS config
            # https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
            if self.profile_name is not None:
                self.session = boto3.session.Session(profile_name=self.profile_name)
            elif self.access_key_id is not None and self.secret_access_key is not None:
                self.session = boto3.session.Session(aws_access_key_id=self.access_key_id, aws_secret_access_key=self.secret_access_key)
            else:
                raise ValueError("AWS profile or access keys must be specified")

            self.client = self.session.client(resource_name, config=self._get_config())
            self.resource = self.session.resource(resource_name, config=self._get_config())

    def get_region(self) -> str:
        return self.session.region_name

    def get_client(self, resource_name: str):
        self._get_client_and_resource(resource_name)
        return self.client

    def get_resource(self, resource_name: str):
        self._get_client_and_resource(resource_name)
        return self.resource
