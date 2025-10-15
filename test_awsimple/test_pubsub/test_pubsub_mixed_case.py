import time
import math

from awsimple import Pub, Sub


def test_pubsub_mixed_case():

    test_channel = "MyTestChannel"  # gets converted to lowercase for SNS topic and SQS queue
    node_name = "MyNodeName"  # gets converted to lowercase for SNS topic and SQS queue
    sent_message = {"MyNumber": 1, "MyBoolean": True, "MyFloat": 2.0 / 3.0}

    pub = Pub(test_channel, node_name)  # specific node name
    pub.start()

    sub = Sub(test_channel, node_name)  # specific node name (must match publisher)
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

    assert received_message is not None
    assert len(received_message) == len(sent_message)
    for key in sent_message:
        received_value = received_message[key]
        sent_value = sent_message[key]
        if isinstance(sent_value, float) and isinstance(received_value, float):
            assert math.isclose(received_value, sent_value)
        else:
            assert received_value == sent_value
