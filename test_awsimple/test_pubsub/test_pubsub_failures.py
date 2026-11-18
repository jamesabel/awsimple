"""
A pubsub thread that fails must be visible to the caller, and a failure it can recover from must not end it.

All tests in this file are AI-generated (Claude Code); the private helpers and constants below are covered by this note.

Each test uses its own channel so it has its own SNS topic and SQS queues, and tags its messages with a run id so messages
left over from an earlier run (possible with real AWS) are ignored.
"""

import json
import logging
import time
import uuid

import pytest
from botocore.exceptions import ClientError

from awsimple import Pub, Sub, SQSAccess, SQSPollAccess, SNSAccess, PubSubFailed, __application_name__
from awsimple.dynamodb import _DynamoDBMetadataTable

_TIMEOUT = 60.0  # seconds


def _client_error(operation_name: str) -> ClientError:
    return ClientError({"Error": {"Code": "TestInducedError", "Message": "induced by the test"}}, operation_name)


def _raise_client_error(*args, **kwargs):
    raise _client_error("TestOperation")


def _do_not_fail_on_error_log(monkeypatch) -> None:
    # The thread logs its failure as an error, which the test session's logging handler (on the root logger) turns into an assert.
    monkeypatch.setattr(logging.getLogger(__application_name__), "propagate", False)


def _publish_until_received(pub: Pub, sub: Sub, run_id: str) -> None:
    """
    Publish until the subscriber gets a message, i.e. its queue is subscribed to the topic and it is polling.
    """
    received = False
    deadline = time.time() + _TIMEOUT
    while not received and time.time() < deadline:
        pub.publish({"run": run_id, "kind": "ready"})
        attempt_deadline = time.time() + 3.0
        while not received and time.time() < attempt_deadline:
            received = any(message.get("run") == run_id for message in sub.get_messages())
            time.sleep(0.1)
    assert received


def _exit(*nodes: Pub | Sub) -> None:
    for node in nodes:
        node.request_exit()
    for node in nodes:
        node.join(_TIMEOUT)
        assert not node.is_alive()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_setup_failure_is_reported_to_publisher(monkeypatch):
    _do_not_fail_on_error_log(monkeypatch)
    monkeypatch.setattr(SNSAccess, "create_topic", _raise_client_error)

    pub = Pub("test_failure_pub_setup", "failure_node")
    pub.raise_if_failed()  # not started yet, so not failed
    pub.start()
    pub.join(_TIMEOUT)  # the thread ends by itself
    assert not pub.is_alive()

    with pytest.raises(PubSubFailed) as exception_info:
        pub.raise_if_failed()
    assert isinstance(exception_info.value.__cause__, ClientError)
    with pytest.raises(PubSubFailed):
        pub.publish({"number": 1})


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_setup_failure_is_reported_to_subscriber(monkeypatch):
    _do_not_fail_on_error_log(monkeypatch)
    monkeypatch.setattr(SNSAccess, "create_topic", _raise_client_error)

    sub = Sub("test_failure_sub_setup", "failure_node")
    sub.start()
    sub.join(_TIMEOUT)  # the thread ends by itself
    assert not sub.is_alive()

    with pytest.raises(PubSubFailed):
        sub.get_messages()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")  # the callback's exception is meant to end the thread
def test_pubsub_callback_exception_ends_subscriber():
    run_id = uuid.uuid4().hex
    channel = "test_failure_callback"

    def callback(message: dict) -> None:
        if message.get("run") == run_id:
            raise ValueError("raised by the test's callback")

    pub = Pub(channel, "failure_pub_node")
    pub.start()
    sub = Sub(channel, "failure_sub_node", sub_callback=callback)
    sub.start()

    deadline = time.time() + _TIMEOUT
    while sub.is_alive() and time.time() < deadline:
        pub.publish({"run": run_id})
        sub.join(3.0)
    assert not sub.is_alive()

    with pytest.raises(PubSubFailed):
        sub.raise_if_failed()

    _exit(pub)
    pub.raise_if_failed()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_unserializable_message_does_not_end_publisher():
    run_id = uuid.uuid4().hex
    good_message = {"run": run_id, "number": 1}

    pub = Pub("test_failure_unserializable", "failure_node")
    pub.start()
    pub.publish({"run": run_id, "not_json": object()})  # logged as a warning and dropped
    pub.publish(good_message)
    _exit(pub)
    pub.raise_if_failed()

    sqs = SQSAccess(pub.sqs_queue_name)
    received = []  # type: list[dict]
    deadline = time.time() + _TIMEOUT
    while len(received) < 1 and time.time() < deadline:
        for sqs_message in sqs.receive_messages():
            message = json.loads(json.loads(sqs_message.message)["Message"])  # unwrap the SNS envelope
            if message.get("run") == run_id:
                received.append(message)
        time.sleep(0.1)
    assert received == [good_message]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_receive_error_does_not_end_subscriber(monkeypatch):
    run_id = uuid.uuid4().hex
    channel = "test_failure_receive"

    original_receive_messages = SQSPollAccess.receive_messages
    failures = []

    def receive_messages(self, *args, **kwargs):
        if len(failures) < 1:
            failures.append(True)
            raise _client_error("ReceiveMessage")
        return original_receive_messages(self, *args, **kwargs)

    monkeypatch.setattr(SQSPollAccess, "receive_messages", receive_messages)

    pub = Pub(channel, "failure_pub_node")
    pub.start()
    sub = Sub(channel, "failure_sub_node")
    sub.start()

    _publish_until_received(pub, sub, run_id)  # the subscriber still receives after its first receive failed
    assert len(failures) == 1

    _exit(pub, sub)
    pub.raise_if_failed()
    sub.raise_if_failed()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_metadata_update_error_does_not_end_subscriber(monkeypatch):
    run_id = uuid.uuid4().hex
    channel = "test_failure_metadata"
    second_message = {"run": run_id, "kind": "second"}

    pub = Pub(channel, "failure_pub_node")
    pub.start()
    sub = Sub(channel, "failure_sub_node")
    sub.start()
    _publish_until_received(pub, sub, run_id)

    # from here on, recording the queue's use time fails; messages must still be delivered
    monkeypatch.setattr(_DynamoDBMetadataTable, "update_table_mtime", _raise_client_error)
    pub.publish(second_message)
    received = False
    deadline = time.time() + _TIMEOUT
    while not received and time.time() < deadline:
        received = second_message in sub.get_messages()
        time.sleep(0.1)
    assert received

    _exit(pub, sub)
    pub.raise_if_failed()
    sub.raise_if_failed()
