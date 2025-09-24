"""
pub/sub abstraction on top of AWS SNS and SQS using boto3.
"""

import time
from typing import Any, Dict, List, Callable
from datetime import timedelta
from multiprocessing import Process, Queue, Event
from threading import Thread
from queue import Empty
from logging import Logger
import json

from typeguard import typechecked
from botocore.exceptions import ClientError

from .sns import SNSAccess
from .sqs import SQSPollAccess, get_all_sqs_queues
from .dynamodb import _DynamoDBMetadataTable
from .platform import get_node_name
from .__version__ import __application_name__

log = Logger(__application_name__)

queue_timeout = timedelta(days=30).total_seconds()


@typechecked()
def remove_old_queues(channel: str) -> list[str]:
    """
    Remove old SQS queues that have not been used recently.
    """
    removed = []
    if len(channel) < 2:  # avoid deleting all queues
        log.warning(f"blank channel ({channel=}) - not deleting any queues")
        return removed
    for sqs_queue_name in get_all_sqs_queues(channel):
        sqs_metadata = _DynamoDBMetadataTable(sqs_queue_name)
        mtime = sqs_metadata.get_table_mtime_f()
        if mtime is not None and time.time() - mtime > queue_timeout:
            sqs = SQSPollAccess(sqs_queue_name)
            try:
                sqs.delete_queue()
                log.info(f'deleted "{sqs_queue_name}",{mtime=}')
            except ClientError:
                log.info(f'"{sqs_queue_name}" already does not exist,{mtime=}')  # already doesn't exist - this is benign
            removed.append(sqs_queue_name)
    return removed


@typechecked()
def _connect_sns_to_sqs(channel_name: str, sqs_queue_name: str, sqs: SQSPollAccess) -> None:
    """
    Connect an SQS queue to an SNS topic.

    :param channel_name: SNS topic name.
    :param sqs_queue_name: SQS queue name.
    :param sqs: SQSPollAccess instance for the SQS queue.
    :return: None
    """

    sqs_arn = sqs.get_arn()

    # Find the topic by name
    sns = SNSAccess(channel_name)
    sns.create_topic()
    topic_arn = sns.get_arn()
    assert sns.resource is not None
    topic = sns.resource.Topic(topic_arn)

    # Subscribe queue to topic
    queue_arn = sqs.get_arn()
    subscription = topic.subscribe(Protocol="sqs", Endpoint=queue_arn)
    log.info(f"Subscribed {sqs_queue_name} to topic {topic_arn}. Subscription ARN: {subscription.arn}")

    # Update queue policy to allow SNS -> SQS
    policy = {
        "Version": "2012-10-17",
        "Id": "sns-sqs-subscription-policy",
        "Statement": [
            {
                "Sid": "Allow-SNS-SendMessage",
                "Effect": "Allow",
                "Principal": {"Service": "sns.amazonaws.com"},
                "Action": "sqs:SendMessage",
                "Resource": sqs_arn,
                "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
            }
        ],
    }
    assert sqs.queue is not None
    sqs.queue.set_attributes(Attributes={"Policy": json.dumps(policy)})
    log.debug(f"Queue {sqs_queue_name} policy updated to allow topic {topic_arn}.")


class _SubscriptionThread(Thread):
    """
    Thread to poll SQS for new messages and put them in a queue for the parent process to read.
    """

    @typechecked()
    def __init__(self, sqs: SQSPollAccess, new_event) -> None:
        super().__init__()
        self._sqs = sqs
        self.new_event = new_event
        self.sub_queue = Queue()  # type: Queue[str]

    def run(self):
        # exit by terminating the parent process
        while True:
            messages = self._sqs.receive_messages()  # long poll
            for message in messages:
                message = json.loads(message.message)
                self.sub_queue.put(message["Message"])
                self.new_event.set()  # notify parent process that a new message is available


class PubSub(Process):

    @typechecked()
    def __init__(self, channel: str, node_name: str = get_node_name(), sub_callback: Callable | None = None) -> None:
        """
        Pub and Sub.
        Create in a separate process to offload from main thread. Also facilitates use of moto mock in tests.

        :param channel: Channel name (SNS topic name).
        :param node_name: Node name (SQS queue name suffix). Defaults to a combination of computer name and username, but can be passed in for customization and/or testing.
        :param sub_callback: Optional thread and process safe callback function to be called when a new message is received. The function should accept a single argument, which will be the message as a dictionary.
        """
        self.channel = channel
        self.node_name = node_name  # e.g., computer name
        self.sub_callback = sub_callback

        self._pub_queue = Queue()  # type: Queue[Dict[str, Any]]
        self._sub_queue = Queue()  # type: Queue[str]

        self._new_event = Event()  # pub or sub sets this when a new message is available or has been sent

        super().__init__()

    def run(self):

        sqs_prefix = f"{self.channel}-"
        sqs_queue_name = f"{sqs_prefix}{self.node_name}"

        sns = SNSAccess(self.channel, auto_create=True)
        sqs_metadata = _DynamoDBMetadataTable(sqs_queue_name)

        sqs = SQSPollAccess(sqs_queue_name)
        if not sqs.exists():
            sqs.create_queue()
            _connect_sns_to_sqs(self.channel, sqs_queue_name, sqs)

        sqs_metadata.update_table_mtime()  # update SQS use time (the existing infrastructure calls it a "table", but we're using it for the SQS queue)
        remove_old_queues(sqs_prefix)  # clean up old queues

        sqs_thread = _SubscriptionThread(sqs, self._new_event)
        sqs_thread.start()

        while True:

            # pub
            try:
                message = self._pub_queue.get(False)
                message_string = json.dumps(message)
                sns.publish(message_string)
            except Empty:
                pass

            # sub
            try:
                message_string = sqs_thread.sub_queue.get(False)
                # if a callback is provided, call it, otherwise put the message in the sub queue for later retrieval
                if self.sub_callback is None:
                    self._sub_queue.put(message_string)
                else:
                    message = json.loads(message_string)
                    self.sub_callback(message)
                sqs_metadata.update_table_mtime()
            except Empty:
                pass  # no message

            # this helps responsiveness
            self._new_event.clear()
            self._new_event.wait(3)  # race conditions can occur, so don't make this wait timeout too long

    @typechecked()
    def publish(self, message: dict) -> None:
        """
        Publish a message.

        :param message: message as a dictionary
        """
        self._pub_queue.put(message)
        self._new_event.set()

    @typechecked()
    def get_messages(self) -> List[Dict[str, Any]]:
        """
        Get all available messages.

        :return: list of messages as dictionaries
        """
        messages = []
        while True:
            try:
                message_string = self._sub_queue.get(block=False)
                message = json.loads(message_string)
                log.debug(f"{message=}")
                messages.append(message)
            except Empty:
                break
        return messages
