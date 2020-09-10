import pytest

from awsimple import SQSPollAccess

from test_awsimple import test_awsimple_str


def test_user_provided_timeout():
    send_message = "hello"
    work_time = 2.0

    q = SQSPollAccess(test_awsimple_str, timeout=round(10.0*work_time), immediate_delete=False, profile_name=test_awsimple_str)
    q.send(send_message)
    receive_message = q.receive_message()
    assert receive_message.message == send_message


def test_user_provided_timeout_nonsensical_parameters():
    send_message = "hello"
    work_time = 2.0

    q = SQSPollAccess(test_awsimple_str, timeout=round(10.0*work_time), profile_name=test_awsimple_str)
    q.send(send_message)
    with pytest.raises(ValueError):
        q.receive_message()
