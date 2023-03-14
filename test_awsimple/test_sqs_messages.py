from pprint import pprint
import time
import math

from ismain import is_main

from awsimple import SQSAccess, SQSPollAccess

from test_awsimple import test_awsimple_str, drain

send_message = "hi"


def test_sqs_immediate_delete():
    drain()

    q = SQSAccess(test_awsimple_str, profile_name=test_awsimple_str)
    q.create_queue()

    send_time = time.time()
    q.send(send_message)
    time.sleep(0.1)

    while (receive_message := q.receive_message()) is None:
        time.sleep(0.1)
    print(receive_message)
    assert receive_message.message == send_message
    print(f"took {time.time() - send_time} seconds")


def test_sqs_poll_immediate_delete():
    drain()

    q = SQSPollAccess(test_awsimple_str, profile_name=test_awsimple_str)
    q.create_queue()

    send_time = time.time()
    q.send(send_message)

    receive_message = q.receive_message()  # will long poll so we expect the message to be available within one call
    assert receive_message is not None
    print(receive_message)
    assert receive_message.message == send_message
    print(f"took {time.time() - send_time} seconds")


def test_sqs_poll_user_delete():
    work_time = 3.0

    drain()

    # populate the run time history
    queue = SQSAccess(test_awsimple_str, immediate_delete=False, profile_name=test_awsimple_str)
    queue.create_queue()
    queue._get_response_history_file_path().unlink(missing_ok=True)
    queue.max_history = 5  # test that we can delete old history values by using a very small history
    for value in range(0, queue.max_history):
        print(value)
        queue.send(str(value))
    while len(messages := queue.receive_messages()) > 0:
        time.sleep(work_time)
        pprint(messages)
        for m in messages:
            print(f"deleting {m.message}")
            m.delete()

    # now do a long poll style
    poll_queue = SQSPollAccess(test_awsimple_str, immediate_delete=False, profile_name=test_awsimple_str)
    poll_queue.create_queue()

    print("sending test message")
    send_time = time.time()
    poll_queue.send(send_message)

    receive_message = poll_queue.receive_message()  # will long poll so we expect the message to be available within one call
    assert receive_message is not None
    print(receive_message.message)
    assert receive_message.message == send_message
    time.sleep(work_time)  # do some work
    print(f"took {time.time() - send_time} seconds")
    receive_message.delete()

    nominal_work_time = poll_queue.calculate_nominal_work_time()
    print(f"{work_time=}, calculated {nominal_work_time=}")
    assert math.isclose(nominal_work_time, work_time, rel_tol=0.5, abs_tol=1.0)  # fairly wide tolerance


def test_sqs_n_messages():
    """
    test for a specific number of messages to be returned
    """

    drain()

    message = "hi"
    queue = SQSAccess(test_awsimple_str)
    queue.create_queue()

    # more than we'll try to take out, and more than the AWS max per call
    for _ in range(0, 14):
        queue.send(message)
    time.sleep(10.0)  # wait for messages to become available

    received = queue.receive_messages(11)  # just over the AWS max per call of 10
    assert len(received) == 11

    drain()  # clean up unreceived messages


if is_main():
    test_sqs_poll_user_delete()
