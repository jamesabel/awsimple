import time

from awsimple import Pub, Sub


def test_pubsub_mixed_case():

    test_channel = "MyTestChannel"  # gets converted to lowercase for SNS topic and SQS queue
    node_name = "MyNodeName"  # gets converted to lowercase for SNS topic and SQS queue
    sent_message = {"MyNumber": 1, "MyBoolean": True, "MyFloat": 2.0 / 3.0}

    pub = Pub(test_channel)
    pub.start()

    sub = Sub(test_channel, node_name, sub_poll=True)
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
    assert not sub.is_alive()

    assert received_message == sent_message
