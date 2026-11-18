"""
A publisher creates no SQS queue: only a subscriber needs one.

All tests in this file are AI-generated (Claude Code).

The test uses its own channel so it has its own SNS topic and SQS queues, and tags its message with a run id so messages
left over from an earlier run (possible with real AWS) are ignored.
"""

import uuid

from awsimple import Pub, SQSAccess

from .listener import make_listener_queue, receive_run_messages, TIMEOUT


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_pubsub_publisher_creates_no_queue():
    run_id = uuid.uuid4().hex
    sent_message = {"run": run_id, "number": 1}

    pub = Pub("test_no_queue_publisher", "no_queue_node")
    listener_queue_name = make_listener_queue(pub.channel)
    pub.start()
    pub.publish(sent_message)
    pub.request_exit()
    pub.join(TIMEOUT)
    assert not pub.is_alive()
    pub.raise_if_failed()

    assert receive_run_messages(listener_queue_name, run_id, 1) == [sent_message]  # it did publish
    assert not SQSAccess(pub.sqs_queue_name).exists()
