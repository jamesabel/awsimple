from awsimple import get_all_sqs_queues, SQSAccess

from awsimple.pubsub import AWS_RESOURCE_PREFIX


def test_pubsub_list_queues():

    # create a queue with the pubsub prefix so this test doesn't depend on queues left over from other tests
    sqs_access = SQSAccess(f"{AWS_RESOURCE_PREFIX}testlistqueues", auto_create=True)
    sqs_access.create_queue()

    queues_names = get_all_sqs_queues()
    print(queues_names)

    found = False
    for queue_name in queues_names:
        if queue_name.startswith(AWS_RESOURCE_PREFIX):
            found = True
    assert found
