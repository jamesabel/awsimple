from dataclasses import dataclass
from threading import Thread

from awsimple import AWSAccess

from typeguard import typechecked


@dataclass
class SQSMessage:
    message: str
    id: str


class SQSAccess(AWSAccess):

    @typechecked(always=True)
    def __init__(self, queue_name: str, **kwargs):
        self.queue_name = queue_name
        super().__init__(resource_name="sqs", **kwargs)

    def create_queue(self):
        raise NotImplementedError

    def read(self) -> (SQSMessage, None):
        raise NotImplementedError

    def write(self, message: str) -> str:
        raise NotImplementedError


class SQSPollAccess(AWSAccess, Thread):
    def run(self) -> (SQSMessage, None):
        raise NotImplementedError
