from multiprocessing import Queue

from awsimple import PubSub, is_mock


def test_pubsub_callback():

    test_channel = "test_channel"
    sent_message = {"number": 1}

    if is_mock():

        message_queue = Queue()

        pubsub = PubSub(test_channel, sub_callback=message_queue.put)  # the callback puts messages in the queue
        pubsub.start()

        pubsub.publish(sent_message)

        # test the callback
        message_from_queue = message_queue.get(timeout=10)
        assert message_from_queue == sent_message

        pubsub.terminate()
        pubsub.join(60)
        assert not pubsub.is_alive()
