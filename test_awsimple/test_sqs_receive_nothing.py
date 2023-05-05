import time
import math

from awsimple import SQSAccess, SQSPollAccess, aws_sqs_long_poll_max_wait_time, is_mock

from test_awsimple import test_awsimple_str, drain

margin = 3.0
rel_tol = 0.2


def test_sqs_receive_nothing():
    drain()
    start = time.time()
    queue = SQSAccess(test_awsimple_str)  # will return immediately
    assert queue.receive_message() is None
    assert len(queue.receive_messages()) == 0
    t = time.time() - start
    print(f"{t=}")
    assert t < 3.0  # "immediate"


def test_sqs_receive_nothing_poll_one():
    if not is_mock():
        drain()
        start = time.time()
        queue = SQSPollAccess(test_awsimple_str)  # will return in AWS SQS max wait time (e.g. 20 sec)
        queue.create_queue()
        assert queue.receive_message() is None

        t = time.time() - start
        print(f"{t=}")
        assert math.isclose(t, aws_sqs_long_poll_max_wait_time + margin, rel_tol=rel_tol, abs_tol=margin)


def test_sqs_receive_nothing_poll_many():
    if not is_mock():
        drain()
        start = time.time()
        queue = SQSPollAccess(test_awsimple_str)  # will return in AWS SQS max wait time (e.g. 20 sec)
        queue.create_queue()
        assert len(queue.receive_messages()) == 0

        t = time.time() - start
        print(f"{t=}")
        assert math.isclose(t, aws_sqs_long_poll_max_wait_time + margin, rel_tol=rel_tol, abs_tol=margin)
