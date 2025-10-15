import time

from awsimple import Pub, Sub


def test_pubsub_get_messages():

    test_channel = "test_channel"
    sent_message = {"number": 1}

    pub = Pub(test_channel)
    pub.start()

    sub = Sub(test_channel, sub_poll=True)
    sub.start()

    pub.publish(sent_message)

    received_message = None
    count = 0
    while count < 600:
        if len(messages := sub.get_messages()) > 0:
            received_message = messages[0]
            break
        time.sleep(0.1)

    pub.request_exit()
    sub.request_exit()
    pub.join(60)
    assert not pub.is_alive()
    sub.join(60)

    assert received_message == sent_message
