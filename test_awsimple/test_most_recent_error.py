import time

from awsimple import SQSAccess, is_mock, is_using_localstack

from test_awsimple import test_awsimple_str, drain
from pytest_socket import disable_socket, enable_socket


def test_most_recent_error():
    message_contents = "hi"

    drain()

    queue = SQSAccess(test_awsimple_str)
    queue.create_queue()
    queue.send(message_contents)

    if not is_mock():
        # emulate a short internet disruption
        disable_socket()

    time.sleep(3)
    message = queue.receive_message()
    if not is_mock() and not is_using_localstack():
        # doesn't work with moto nor localstack :(
        assert message.message == message_contents

    if not is_mock():
        enable_socket()

    if is_mock():
        assert queue.most_recent_error is None
    else:
        print(f"{queue.most_recent_error=}")  # disable_socket() doesn't seem to work for this case - somehow we get the message anyway

    drain()
