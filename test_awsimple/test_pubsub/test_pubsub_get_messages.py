import time

from awsimple import PubSub, is_mock


def test_pubsub_get_messages():

    test_channel = "test_channel"
    sent_message = {"number": 1}

    if is_mock():

        pubsub = PubSub(test_channel)
        pubsub.start()

        pubsub.publish(sent_message)

        received_message = None
        count = 0
        while count < 600:
            if len(messages := pubsub.get_messages()) > 0:
                received_message = messages[0]
                break
            time.sleep(0.1)

        pubsub.terminate()
        pubsub.join(60)
        assert not pubsub.is_alive()

        assert received_message == sent_message
