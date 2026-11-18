"""
Test helper: an SQS queue subscribed to a pubsub channel's SNS topic, to see what a publisher sent without using a Sub.

This file is AI-generated (Claude Code).
"""

import json
import time

from awsimple import SQSAccess, SQSPollAccess, SNSAccess
from awsimple.pubsub import _connect_sns_to_sqs

TIMEOUT = 60.0  # seconds


def make_listener_queue(topic_name: str) -> str:
    """
    Create (if it does not exist yet) a queue subscribed to the topic. Do this before the publisher sends: SNS does not retain messages.

    :param topic_name: the SNS topic name, i.e. the "channel" attribute of a Pub or Sub
    :return: the queue's name
    """
    queue_name = f"{topic_name}listener"
    sqs = SQSPollAccess(queue_name)
    if not sqs.exists():
        sqs.create_queue()
    _connect_sns_to_sqs(sqs, SNSAccess(topic_name))  # also creates the topic
    return queue_name


def receive_run_messages(queue_name: str, run_id: str, expected_count: int) -> list[dict]:
    """
    Read the queue until the expected number of messages tagged with this run id have arrived (SNS to SQS delivery is
    asynchronous on real AWS). Messages left over from an earlier run (possible with real AWS) are ignored.
    """
    sqs = SQSAccess(queue_name)  # short poll
    received = []  # type: list[dict]
    deadline = time.time() + TIMEOUT
    while len(received) < expected_count and time.time() < deadline:
        for sqs_message in sqs.receive_messages():
            message = json.loads(json.loads(sqs_message.message)["Message"])  # unwrap the SNS envelope
            if message.get("run") == run_id:
                received.append(message)
        if len(received) < expected_count:
            time.sleep(0.1)
    return received
