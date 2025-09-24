import time

from awsimple import SQSAccess

from test_awsimple import test_awsimple_str


def test_sqs_auto_create():
    # have to wait 60 seconds from delete to (re)creation so don't use the same queue name as other tests
    queue_name = "auto_create"
    q = SQSAccess(queue_name, profile_name=test_awsimple_str, auto_create=True)
    count = 0
    while not q.exists() and count < 20:
        time.sleep(3)
        count += 1
    exists = q.exists()
    assert exists

    arn = q.get_arn()
    assert queue_name in arn

    q.delete_queue()
