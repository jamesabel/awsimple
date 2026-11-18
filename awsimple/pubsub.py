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
from botocore.exceptions import ClientError, BotoCoreError
import strif

from .sns import SNSAccess
from .sqs import SQSPollAccess, get_all_sqs_queues
from .dynamodb import _DynamoDBMetadataTable, DBItemNotFound, DynamoDBTableNotFound
from .platform import get_node_name
from .__version__ import __application_name__
from .exceptions import PubSubFailed

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
            except (ClientError, BotoCoreError) as e:
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
        :param sub_callback: Optional thread and process safe callback function to be called when a new message is received.
            The function should accept a single argument, which will be the message as a dictionary.
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

        self._failure = None  # type: Exception | None  # the exception that ended the thread, if any

        super().__init__(daemon=True)  # make daemon so an instance of this thread exits when the main program exits

    def _is_subscriber(self) -> bool:
        return self.sub_callback is not None or self.use_sub_queue

    def run(self):
        try:
            self._run()
        except Exception as e:
            # A daemon thread that dies silently leaves publish() queueing into the void, and an exception can not propagate
            # from this thread to the application's. Record whatever ended the thread (an AWS error, or anything the
            # subscription callback raised), so that raise_if_failed(), publish() and get_messages() raise it, as the cause of
            # a PubSubFailed, on the application's thread.
            self._failure = e
            log.error(f"pubsub thread failed,{self.channel=},{e!r}")
            raise

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

        if self._is_subscriber():
            sqs_thread, sqs_metadata = self._start_subscription()
        else:
            # publish only: no SQS queue is needed (one would just collect messages that nobody reads)
            sqs_thread = None
            sqs_metadata = None

        try:
            while not self._exit_event.is_set():
                self._drain_pub_queue(sns)
                if sqs_thread is not None and sqs_metadata is not None:
                    self._drain_sub_queue(sqs_thread, sqs_metadata)

                if self._new_event.wait(self._new_event_wait_time):  # timeout in case the new event technique fails
                    self._new_event.clear()

            # Send whatever was published before the exit request: the loop above only drains while the exit event is clear.
            # This is done before stopping the subscription thread, which can take as long as an SQS long poll.
            self._drain_pub_queue(sns)
        finally:
            # also when this thread is ending on an error: otherwise the subscription thread would go on taking messages off the SQS queue with nobody to deliver them
            if sqs_thread is not None:
                sqs_thread.request_exit()

        if sqs_thread is not None and sqs_metadata is not None:
            sqs_thread.join(30)
            if sqs_thread.is_alive():
                log.error("sqs_thread did not exit cleanly")
            # Deliver what the subscription thread received up to its exit. Those messages are already deleted from the
            # SQS queue, so they would otherwise be lost.
            self._drain_sub_queue(sqs_thread, sqs_metadata)
            self._drain_pub_queue(sns)  # a subscription callback may have published during the drain above

    def _start_subscription(self) -> tuple[_SubscriptionThread, _DynamoDBMetadataTable]:
        """
        Create this node's SQS queue (if it does not exist yet), subscribe it to the topic and start the thread that polls it.

        :return: the subscription thread and the queue's metadata (its last use time)
        """
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

        sqs_thread = _SubscriptionThread(sqs, self._new_event)
        sqs_thread.start()
        return sqs_thread, sqs_metadata

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
            except (TypeError, ValueError, ClientError, BotoCoreError) as e:
                # TypeError, ValueError: the message is not JSON serializable
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
            if self.use_sub_queue:
                self._sub_queue.put(message_string)
            if self.sub_callback is not None:
                try:
                    message = json.loads(message_string)
                except json.JSONDecodeError as e:
                    log.warning(f"malformed message,{self.sqs_queue_name=},{e}")
                else:
                    self.sub_callback(message)  # an exception raised by the callback is not caught here: it ends the thread (see run())
        if got_message:
            try:
                sqs_metadata.update_table_mtime()
            except (ClientError, BotoCoreError) as e:
                log.warning(f"SQS metadata update failed,{self.sqs_queue_name=},{e}")

    @typechecked()
    def publish(self, message: dict) -> None:
        """
        Publish a message.

        The message is queued and sent to the SNS topic by this object's thread, so it has not been sent when this call returns.
        A message only reaches the subscribers (nodes) whose SQS queues are subscribed to the topic when it is sent. A node's
        queue is created the first time a Sub is started for that node and then stays, so a subscriber that is not running
        gets the message when it next starts. A subscriber starting for the first time does not get a message that was sent
        before it finished subscribing: SNS does not retain messages, and a publisher can not know which subscribers are
        still starting (they are usually in other processes or applications).
        To flush before exiting, call request_exit() and then join(): a send has been attempted for every message published
        before request_exit() by the time join() returns. A send that fails is logged as a warning and the message is dropped.
        A message published after request_exit() may not be sent.

        :param message: message as a dictionary
        :raises PubSubFailed: if the thread has failed, so the message can not be sent
        """
        self.raise_if_failed()
        if self._exit_event.is_set():
            log.warning(f"published after request_exit(), so the message may not be sent,{self.channel=}")
        self._pub_queue.put(message)
        self._new_event.set()

    @typechecked()
    def get_messages(self) -> List[Dict[str, Any]]:
        """
        Get all available messages. Use set sub_poll=True when creating the PubSub object to use this function.

        :return: list of messages as dictionaries
        :raises PubSubFailed: if the thread has failed and there are no more messages to return
        """
        if not self.use_sub_queue:
            raise RuntimeError("use_sub_queue must be True to use get_messages()")
        if self._sub_queue.empty():
            self.raise_if_failed()
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

    def raise_if_failed(self) -> None:
        """
        Raise PubSubFailed if the thread has ended on an error (e.g. AWS setup failed, or the subscription callback raised).
        The exception that ended the thread is the PubSubFailed's cause (its __cause__ attribute).
        Does nothing while the thread is running or once it has exited normally. Call this after join() to check that the
        thread, including its final sends, ran to completion.

        :raises PubSubFailed: if the thread has failed
        """
        if self._failure is not None:
            raise PubSubFailed(self.channel, repr(self._failure)) from self._failure

    def request_exit(self) -> None:
        """
        Request the thread to exit. This returns immediately; call join() to wait for the thread to finish.

        Before it finishes, the thread sends the messages published so far and delivers (to the callback or get_messages()) the
        messages it has already received. join() therefore waits for those sends and, for a subscriber, for the SQS long poll
        in progress (up to 20 seconds). Pass join() a timeout for a bounded shutdown. join() does not report a failure of the
        thread; call raise_if_failed() after it for that.
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
        Pub only. Publishes to the channel's SNS topic; it does not create an SQS queue (only a Sub does).
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
