"""
pub/sub abstraction on top of AWS SNS and SQS using boto3.
"""

import time
from functools import lru_cache
from typing import Any, Dict, List, Callable
from datetime import timedelta
from threading import Thread, Event
from queue import Queue
from queue import Empty
from logging import getLogger
import json

from typeguard import typechecked
from botocore.exceptions import ClientError
import strif

from .sns import SNSAccess
from .sqs import SQSPollAccess, get_all_sqs_queues
from .dynamodb import _DynamoDBMetadataTable, DBItemNotFound, DynamoDBTableNotFound
from .platform import get_node_name
from .__version__ import __application_name__

log = getLogger(__application_name__)  # getLogger (not a raw Logger instance) so the application's logging configuration applies

queue_timeout = timedelta(days=30).total_seconds()

SQS_NAME = "sqs"

AWS_RESOURCE_PREFIX = "ps"  # for pubsub


@typechecked()
def remove_old_queues(
    channel: str, profile_name: str | None = None, aws_access_key_id: str | None = None, aws_secret_access_key: str | None = None, region_name: str | None = None
) -> list[str]:
    """
    Remove old SQS queues that have not been used recently.
    """
    removed = []  # type: list[str]
    if len(channel) < 2:  # avoid deleting all queues
        log.warning(f"blank channel ({channel=}) - not deleting any queues")
        return removed
    for sqs_queue_name in get_all_sqs_queues(channel, profile_name=profile_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name):
        sqs_metadata = _DynamoDBMetadataTable(
            SQS_NAME, sqs_queue_name, profile_name=profile_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name
        )
        try:
            mtime = sqs_metadata.get_table_mtime_f()
        except (DBItemNotFound, DynamoDBTableNotFound):
            mtime = None  # queue has no metadata entry (e.g. it was never used) - leave it alone
        if mtime is not None and time.time() - mtime > queue_timeout:
            sqs = SQSPollAccess(sqs_queue_name, profile_name=profile_name, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
            try:
                sqs.delete_queue()
                log.info(f'deleted "{sqs_queue_name}",{mtime=}')
            except ClientError:
                log.info(f'"{sqs_queue_name}" already does not exist,{mtime=}')  # already doesn't exist - this is benign
            removed.append(sqs_queue_name)
    return removed


@typechecked()
def _connect_sns_to_sqs(sqs: SQSPollAccess, sns: SNSAccess) -> None:
    """
    Connect an SQS queue to an SNS topic.

    :param sqs: SQS access object
    :param sns: SNS access object
    :return: None
    """

    sqs_arn = sqs.get_arn()

    # Find the topic by name
    sns.create_topic()
    topic_arn = sns.get_arn()
    assert sns.resource is not None
    topic = sns.resource.Topic(topic_arn)

    # Subscribe queue to topic
    subscription = topic.subscribe(Protocol="sqs", Endpoint=sqs_arn)
    log.info(f"Subscribed {sqs.queue_name} to topic {topic_arn}. Subscription ARN: {subscription.arn}")

    # Update queue policy to allow SNS -> SQS (merge into any existing policy - replacing it would revoke other topics' permissions)
    statement = {
        "Sid": f"AllowSNSSendMessage{topic_arn.split(':')[-1]}",  # Sid must be unique within the policy, so include the topic name
        "Effect": "Allow",
        "Principal": {"Service": "sns.amazonaws.com"},
        "Action": "sqs:SendMessage",
        "Resource": sqs_arn,
        "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
    }
    assert sqs.queue is not None
    existing_policy_string = sqs.client.get_queue_attributes(QueueUrl=sqs.queue.url, AttributeNames=["Policy"]).get("Attributes", {}).get("Policy")
    if existing_policy_string is None:
        policy = {"Version": "2012-10-17", "Id": "sns-sqs-subscription-policy", "Statement": [statement]}
    else:
        policy = json.loads(existing_policy_string)
        statements = policy.setdefault("Statement", [])
        if statement not in statements:
            statements.append(statement)
    sqs.queue.set_attributes(Attributes={"Policy": json.dumps(policy)})
    log.debug(f"Queue {sqs.queue_name} policy updated to allow topic {topic_arn}.")


class _SubscriptionThread(Thread):
    """
    Thread to poll SQS for new messages and put them in a queue for the parent thread to read.
    """

    @typechecked()
    def __init__(self, sqs: SQSPollAccess, new_event: Event) -> None:
        super().__init__(daemon=True)
        self._sqs = sqs
        self.sub_queue = Queue()  # type: Queue[str]
        self._exit_event = Event()
        self._new_event = new_event

    def run(self):
        while not self._exit_event.is_set():
            try:
                messages = self._sqs.receive_messages()  # long poll
            except Exception as e:
                # don't let a transient AWS error silently kill the polling thread
                log.warning(f"{self._sqs.queue_name=} receive failed : {e}")
                time.sleep(1.0)
                continue
            for message in messages:
                try:
                    parsed = json.loads(message.message)
                    self.sub_queue.put(parsed["Message"])
                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    log.warning(f"{self._sqs.queue_name=} malformed message : {e}")
                else:
                    self._new_event.set()

    def request_exit(self):
        self._exit_event.set()


@lru_cache
def make_name_aws_safe(*args: str) -> str:
    """
    Make a name safe for an SQS queue to subscribe to an SNS topic. This ensures we adhere to name restrictions such as acceptable characters and length.

    :params: input name(s)
    :return: AWS safe name
    """

    # join with a separator so e.g. ("a", "b") and ("ab",) hash differently (the separator can't appear in the hash output, avoiding cross-boundary collisions)
    base36 = strif.hash_string("\x1f".join(args)).base36.strip()
    assert 30 <= len(base36) <= 31
    return base36


class _PubSub(Thread):

    @typechecked()
    def __init__(
        self,
        channel: str,
        node_name: str | None,
        sub_callback: Callable | None,
        use_sub_queue: bool,
        profile_name: str | None,
        aws_access_key_id: str | None,
        aws_secret_access_key: str | None,
        region_name: str | None,
    ) -> None:
        """
        Pub and Sub.
        Create in a separate process to offload from main thread. Also facilitates use of moto mock in tests.

        :param channel: Channel name (used for SNS topic name). This must not be a prefix of other channel names to avoid collisions (don't name one channel "a" and another "ab").
        :param node_name: Node name (SQS queue name suffix). Defaults to a combination of computer name and username, but can be passed in for customization and/or testing.
        :param sub_callback: Optional thread and process safe callback function to be called when a new message is received. The function should accept a single argument, which will be the message as a dictionary.
        :param use_sub_queue: If True, use an internal queue to store received messages. If False, messages must be handled by the callback function. Default is False.
        """
        self.channel = AWS_RESOURCE_PREFIX + make_name_aws_safe(channel)  # prefix with ps (pubsub) to avoid collisions with other uses of SNS topics and SQS queues
        self.node_name = get_node_name() if node_name is None else node_name
        # queue name is the channel name plus a node hash, so all of a channel's queues share the channel name as a prefix (this is what lets remove_old_queues() find them)
        self.sqs_queue_name = self.channel + make_name_aws_safe(self.node_name)
        self.sub_callback = sub_callback
        self.use_sub_queue = use_sub_queue

        self.profile_name = profile_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name

        self._pub_queue = Queue()  # type: Queue[Dict[str, Any]]
        self._sub_queue = Queue()  # type: Queue[str]

        self._exit_event = Event()  # set this to request exit
        self._new_event = Event()
        self._new_event_wait_time = 10  # seconds

        super().__init__(daemon=True)  # make daemon so an instance of this thread exits when the main program exits

    def run(self):
        try:
            self._run()
        except Exception:
            # a daemon thread that dies silently leaves publish() queueing into the void, so at least make the failure visible
            log.exception(f"pubsub thread failed,{self.channel=}")

    def _run(self):

        sns = SNSAccess(
            self.channel,
            auto_create=True,
            profile_name=self.profile_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )
        sns.create_topic()

        sqs_metadata = _DynamoDBMetadataTable(
            SQS_NAME,
            self.sqs_queue_name,
            profile_name=self.profile_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )

        sqs = SQSPollAccess(
            self.sqs_queue_name, profile_name=self.profile_name, aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key, region_name=self.region_name
        )
        if not sqs.exists():
            sqs.create_queue()
            sns = SNSAccess(
                topic_name=self.channel,
                profile_name=self.profile_name,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                region_name=self.region_name,
            )
            _connect_sns_to_sqs(sqs, sns)

        sqs_metadata.update_table_mtime()  # update SQS use time (the existing infrastructure calls it a "table", but we're using it for the SQS queue)
        # clean up old queues (using the same credentials as this instance)
        remove_old_queues(
            self.channel,
            profile_name=self.profile_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            region_name=self.region_name,
        )

        if self.sub_callback is None and not self.use_sub_queue:
            # not being used as a subscriber
            sqs_thread = None
        else:
            sqs_thread = _SubscriptionThread(sqs, self._new_event)
            sqs_thread.start()

        while not self._exit_event.is_set():

            self._drain_pub_queue(sns)
            if sqs_thread is not None:
                self._drain_sub_queue(sqs_thread, sqs_metadata)

            if self._new_event.wait(self._new_event_wait_time):  # timeout in case the new event technique fails
                self._new_event.clear()

        if sqs_thread is not None:
            sqs_thread.request_exit()
            sqs_thread.join(30)
            if sqs_thread.is_alive():
                log.error("sqs_thread did not exit cleanly")

    def _drain_pub_queue(self, sns: SNSAccess) -> None:
        # drain all queued messages (the "new" event is binary, so handling only one message per wait cycle would throttle bursts to one message per timeout)
        while True:
            try:
                message = self._pub_queue.get(False)
            except Empty:
                break
            try:
                message_string = json.dumps(message)
                sns.publish(message_string)
            except Exception as e:
                log.warning(f"SNS publish failed,{self.channel=},{e}")

    def _drain_sub_queue(self, sqs_thread: _SubscriptionThread, sqs_metadata: _DynamoDBMetadataTable) -> None:
        # drain all received messages
        got_message = False
        while True:
            try:
                message_string = sqs_thread.sub_queue.get(False)
            except Empty:
                break
            got_message = True
            try:
                if self.use_sub_queue:
                    self._sub_queue.put(message_string)
                if self.sub_callback is not None:
                    message = json.loads(message_string)
                    self.sub_callback(message)
            except Exception as e:
                log.warning(f"SQS,{self.sqs_queue_name=},{e}")
        if got_message:
            try:
                sqs_metadata.update_table_mtime()
            except Exception as e:
                log.warning(f"SQS metadata update failed,{self.sqs_queue_name=},{e}")

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
        Get all available messages. Use set sub_poll=True when creating the PubSub object to use this function.

        :return: list of messages as dictionaries
        """
        if not self.use_sub_queue:
            raise RuntimeError("use_sub_queue must be True to use get_messages()")
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

    def request_exit(self) -> None:
        """
        Request the process to exit.
        """
        self._exit_event.set()
        self._new_event.set()


class Pub(_PubSub):

    @typechecked()
    def __init__(
        self,
        channel: str,
        node_name: str | None = None,
        profile_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        region_name: str | None = None,
    ) -> None:
        """
        Pub only.
        Create in a separate process to offload from main thread. Also facilitates use of moto mock in tests.

        :param channel: Channel name (used for SNS topic name). This must not be a prefix of other channel names to avoid collisions (don't name one channel "a" and another "ab").
        """
        super().__init__(
            channel=channel,
            node_name=node_name,
            sub_callback=None,  # pub only
            use_sub_queue=False,  # pub only
            profile_name=profile_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )


class Sub(_PubSub):

    @typechecked()
    def __init__(
        self,
        channel: str,
        node_name: str | None = None,
        sub_callback: Callable | None = None,
        profile_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        region_name: str | None = None,
    ) -> None:
        """
        Sub only.
        Create in a separate process to offload from main thread. Also facilitates use of moto mock in tests.

        :param channel: Channel name (used for SNS topic name). This must not be a prefix of other channel names to avoid collisions (don't name one channel "a" and another "ab").
        :param node_name: Node name (SQS queue name suffix). Defaults to a combination of computer name and username, but can be passed in for customization and/or testing.
        :param sub_callback: Optional callback function to be called when a new message is received. The function should accept a single argument, which will be the message as a dictionary.
                             If this is not used, then get_messages() should be used to retrieve messages.
        """
        super().__init__(
            channel=channel,
            node_name=node_name,
            sub_callback=sub_callback,
            use_sub_queue=sub_callback is None,  # if no callback, use internal queue
            profile_name=profile_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name,
        )
