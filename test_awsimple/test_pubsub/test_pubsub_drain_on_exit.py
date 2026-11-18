"""
Messages published or received around request_exit() must not be dropped.

All tests in this file are AI-generated (Claude Code); the private helpers and constants below are covered by this note.

Each test uses its own channel so it has its own SNS topic and SQS queues, and tags its messages with a run id so messages
left over from an earlier run (possible with real AWS) are ignored.
"""

import logging
import time
import uuid

from awsimple import Pub, Sub, __application_name__

from .listener import make_listener_queue, receive_run_messages, TIMEOUT

_TIMEOUT = TIMEOUT


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_publish_then_exit_during_setup():
    run_id = uuid.uuid4().hex
    sent_message = {"run": run_id, "number": 1}

    pub = Pub("test_drain_exit_during_setup", "drain_node")
    listener_queue_name = make_listener_queue(pub.channel)
    pub.start()
    # the thread is still doing its AWS setup, so the exit request is already set when its loop is first tested
    pub.publish(sent_message)
    pub.request_exit()
    pub.join(_TIMEOUT)
    assert not pub.is_alive()

    assert receive_run_messages(listener_queue_name, run_id, 1) == [sent_message]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_publish_then_exit_while_waiting():
    run_id = uuid.uuid4().hex
    first_message = {"run": run_id, "number": 1}
    second_message = {"run": run_id, "number": 2}

    pub = Pub("test_drain_exit_while_waiting", "drain_node")
    listener_queue_name = make_listener_queue(pub.channel)
    pub.start()

    # once the first message has arrived, setup has finished and the thread is in its loop, waiting for an event
    pub.publish(first_message)
    assert receive_run_messages(listener_queue_name, run_id, 1) == [first_message]

    pub.publish(second_message)
    pub.request_exit()
    pub.join(_TIMEOUT)
    assert not pub.is_alive()

    assert receive_run_messages(listener_queue_name, run_id, 1) == [second_message]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_publish_burst_then_exit():
    run_id = uuid.uuid4().hex
    sent_messages = [{"run": run_id, "number": number} for number in range(25)]  # more than one SQS receive (10) can return

    pub = Pub("test_drain_burst", "drain_node")
    listener_queue_name = make_listener_queue(pub.channel)
    pub.start()
    for sent_message in sent_messages:
        pub.publish(sent_message)
    pub.request_exit()
    pub.join(_TIMEOUT)
    assert not pub.is_alive()

    received_messages = receive_run_messages(listener_queue_name, run_id, len(sent_messages))
    # standard SNS topics and SQS queues do not guarantee ordering, so compare without regard to order
    assert sorted(received_messages, key=lambda message: message["number"]) == sent_messages


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_message_received_during_exit_is_delivered():
    run_id = uuid.uuid4().hex
    channel = "test_drain_received_during_exit"
    late_message = {"run": run_id, "kind": "late"}

    pub = Pub(channel, "drain_pub_node")
    pub.start()
    sub = Sub(channel, "drain_sub_node")
    sub.start()

    # publish until the subscriber gets a message, i.e. its queue is subscribed to the topic and it is polling
    subscribed = False
    deadline = time.time() + _TIMEOUT
    while not subscribed and time.time() < deadline:
        pub.publish({"run": run_id, "kind": "ready"})
        attempt_deadline = time.time() + 3.0
        while not subscribed and time.time() < attempt_deadline:
            subscribed = any(message.get("run") == run_id for message in sub.get_messages())
            time.sleep(0.1)
    assert subscribed

    time.sleep(1.0)  # let any remaining "ready" messages through, so the subscriber is back in a long poll
    sub.get_messages()

    # The subscriber's main loop ends at once, but its SQS thread is in a long poll, which takes the late message off the
    # SQS queue (deleting it there). The message must still be delivered rather than dropped.
    sub.request_exit()
    time.sleep(0.5)
    pub.publish(late_message)
    sub.join(_TIMEOUT)
    assert not sub.is_alive()

    assert late_message in sub.get_messages()

    pub.request_exit()
    pub.join(_TIMEOUT)
    assert not pub.is_alive()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_publish_after_exit_warns(caplog):
    pub = Pub("test_drain_publish_after_exit", "drain_node")
    pub.start()
    pub.request_exit()
    pub.join(_TIMEOUT)
    assert not pub.is_alive()

    with caplog.at_level(logging.WARNING, logger=__application_name__):
        pub.publish({"number": 1})
    assert any("request_exit" in record.getMessage() for record in caplog.records)
