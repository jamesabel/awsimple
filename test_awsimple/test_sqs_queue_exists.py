from awsimple import SQSAccess

from test_awsimple import test_awsimple_str


def test_sqs_queue_exists():
    q = SQSAccess(test_awsimple_str)
    q.create_queue()
    assert q.exists()
    assert not SQSAccess("IDoNotExist").exists()
