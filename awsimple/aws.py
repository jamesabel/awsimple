import logging
from dataclasses import dataclass

import boto3
from botocore.config import Config

from awsimple import __application_name__

log = logging.getLogger(__application_name__)


@dataclass
class AWSAccess:
    profile_name: str = None
    access_key_id: str = None
    secret_access_key: str = None

    def _get_config(self):
        timeout = 60 * 60  # AWS default is 60
        return Config(connect_timeout=timeout, read_timeout=timeout)

    def _get_session(self):
        # use keys in AWS config
        # https://docs.aws.amazon.com/cli/latest/userguide/cli-config-files.html
        if self.profile_name is not None:
            session = boto3.session.Session(profile_name=self.profile_name)
        elif self.access_key_id is not None and self.secret_access_key is not None:
            session = boto3.session.Session(aws_access_key_id=self.access_key_id, aws_secret_access_key=self.secret_access_key)
        else:
            raise ValueError("AWS profile or access keys must be specified")
        return session

    def get_resource(self, resource_name: str):
        session = self._get_session()
        return session.resource(resource_name, config=self._get_config())

    def get_client(self, resource_name: str):
        session = self._get_session()
        return session.client(resource_name,  config=self._get_config())

    def get_region(self) -> str:
        session = self._get_session()
        return session.region_name
