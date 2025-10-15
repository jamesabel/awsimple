from awsimple import get_all_sqs_queues, is_mock

from awsimple.pubsub import AWS_RESOURCE_PREFIX


def test_pubsub_list_queues():

    queues_names = get_all_sqs_queues()
    print(queues_names)

    if not is_mock():
        found = False
        for queue_name in queues_names:
            if queue_name.startswith(AWS_RESOURCE_PREFIX):
                found = True
        assert found
