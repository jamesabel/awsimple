from ismain import is_main

from awsimple import SQSAccess

from test_awsimple import test_awsimple_str


def test_sqs_create_and_delete_queue():
    # have to wait 60 seconds from delete to (re)creation so don't use the same queue name as other tests
    queue_name = "createdelete"
    q = SQSAccess(queue_name, profile_name=test_awsimple_str)
    url = q.create_queue()
    print(url)

    # something like https://us-west-2.queue.amazonaws.com/076966278319/createdelete
    assert len(url) > 10
    assert url.endswith(queue_name)
    assert url.startswith("https://")
    assert "aws" in url

    q.delete_queue()


if is_main():
    test_sqs_create_and_delete_queue()
