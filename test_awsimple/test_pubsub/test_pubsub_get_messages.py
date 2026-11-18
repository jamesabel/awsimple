import time

from awsimple.pubsub import Pub, Sub


def test_pubsub_get_messages():

    test_channel = "test_channel"
    sent_message = {"number": 1}

    pub = Pub(test_channel)
    pub.start()

    sub = Sub(test_channel)
    sub.start()

    # SNS does not retain messages, so a message sent before the subscriber has subscribed is lost: publish until one is received
    received_message = None
    count = 0
    while count < 600:
        if count % 10 == 0:
            pub.publish(sent_message)
        if len(messages := sub.get_messages()) > 0:
            received_message = messages[0]
            break
        time.sleep(0.1)
        count += 1

    pub.request_exit()
    sub.request_exit()
    pub.join(60)
    sub.join(60)
    assert not pub.is_alive()
    assert not sub.is_alive()

    assert received_message == sent_message
