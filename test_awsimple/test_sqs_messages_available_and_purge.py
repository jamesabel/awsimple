import time

from awsimple import SQSAccess, is_mock

from test_awsimple import test_awsimple_str, drain


def wait_for_n_messages_available(queue: SQSAccess, expected_number_of_messages: int):
    time_out = 0
    while (messages_available := queue.messages_available()) != expected_number_of_messages and time_out < 60:
        time_out += 1
        time.sleep(1.0)
    assert messages_available == expected_number_of_messages


def test_sqs_message_available_and_purge():
    if not is_mock():
        drain()

        queue = SQSAccess(test_awsimple_str)
        queue.create_queue()

        wait_for_n_messages_available(queue, 0)

        for number_of_messages in range(1, 5):
            queue.send(str(number_of_messages))
            wait_for_n_messages_available(queue, number_of_messages)

        queue.purge()
        wait_for_n_messages_available(queue, 0)
