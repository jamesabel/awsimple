from awsimple import SQSAccess, is_mock

from test_awsimple import test_awsimple_str


def test_sqs_queue_exists():
    q = SQSAccess(test_awsimple_str)
    q.create_queue()
    queue_exists = q.exists()
    # doesn't work with moto :(
    if not is_mock():
        assert queue_exists
        queue_exists = SQSAccess("IDoNotExist").exists()
        assert not queue_exists
