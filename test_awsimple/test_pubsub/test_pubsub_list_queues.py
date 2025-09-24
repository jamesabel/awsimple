import time

from awsimple import PubSub, is_mock, get_all_sqs_queues


def test_pubsub_list_queues():

    test_channel = "test_channel"
    sent_message = {"number": 1}

    # only works on real AWS
    if not is_mock():

        pubsub = PubSub(test_channel)
        pubsub.start()

        pubsub.publish(sent_message)

        while len(pubsub.get_messages()) < 1:
            time.sleep(0.1)

        pubsub.terminate()

        queues_names = get_all_sqs_queues()
        print(queues_names)
        found = False
        for queue_name in queues_names:
            if queue_name.startswith(test_channel):
                found = True
        assert found
