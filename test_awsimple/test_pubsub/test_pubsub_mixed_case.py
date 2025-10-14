import time

from awsimple import PubSub


def test_pubsub_mixed_case():

    test_channel = "MyTestChannel"  # gets converted to lowercase for SNS topic and SQS queue
    node_name = "MyNodeName"  # gets converted to lowercase for SNS topic and SQS queue
    sent_message = {"MyNumber": 1, "MyBoolean": True, "MyFloat": 2.0 / 3.0}

    pubsub = PubSub(test_channel, node_name)
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
