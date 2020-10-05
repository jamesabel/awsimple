import pytest
import time

from awsimple import SQSPollAccess, SQSAccess, is_mock

from test_awsimple import test_awsimple_str, drain


def test_user_provided_timeout():

    drain()

    send_message = "hello"
    work_time = 2.0

    qp = SQSPollAccess(test_awsimple_str, visibility_timeout=round(10.0 * work_time), immediate_delete=False, profile_name=test_awsimple_str)
    qp.create_queue()
    qp.send(send_message)
    time.sleep(1.0)
    receive_message = qp.receive_message()
    assert receive_message.message == send_message

    q = SQSAccess(test_awsimple_str, profile_name=test_awsimple_str)
    q.create_queue()
    assert q.receive_message() is None  # make sure the message is now invisible

    if not is_mock():
        receive_message.delete()  # not working for mock todo: fix

    assert q.receive_message() is None


def test_user_provided_minimum_timeout():

    drain()

    send_message = "hello"
    work_time = 2.0

    qp = SQSPollAccess(test_awsimple_str, minimum_visibility_timeout=round(10.0 * work_time), immediate_delete=False, profile_name=test_awsimple_str)
    qp.create_queue()
    qp.send(send_message)
    time.sleep(1.0)
    receive_message = qp.receive_message()
    assert receive_message.message == send_message

    q = SQSAccess(test_awsimple_str, profile_name=test_awsimple_str)
    q.create_queue()
    assert q.receive_message() is None  # make sure the message is now invisible

    if not is_mock():
        receive_message.delete()  # not working for mock todo: fix

    assert q.receive_message() is None


def test_actually_timeout():

    drain()

    send_message = "hello"
    work_time = 5.0

    qp = SQSPollAccess(test_awsimple_str, visibility_timeout=round(0.5 * work_time), immediate_delete=False, profile_name=test_awsimple_str)
    qp.create_queue()
    qp.send(send_message)
    time.sleep(1.0)
    receive_message = qp.receive_message()
    assert receive_message.message == send_message  # got it once

    q = SQSAccess(test_awsimple_str, profile_name=test_awsimple_str)
    assert q.receive_message() is None  # make sure the message is now invisible
    time.sleep(work_time)  # will take "too long", so message should be available again on next receive_message

    if not is_mock():
        # not working for mock todo: fix
        assert qp.receive_message().message == send_message
        receive_message.delete()  # now we delete it

    assert q.receive_message() is None


def test_user_provided_timeout_nonsensical_parameters():

    drain()

    send_message = "hello"
    work_time = 2.0

    q = SQSPollAccess(test_awsimple_str, visibility_timeout=round(10.0 * work_time), profile_name=test_awsimple_str)
    q.create_queue()
    q.send(send_message)
    with pytest.raises(ValueError):
        q.receive_message()
