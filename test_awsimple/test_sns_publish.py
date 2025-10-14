from awsimple import SNSAccess, SQSPollAccess

from test_awsimple import test_awsimple_str, drain


def test_sns_publish():
    drain()

    sqs_access = SQSPollAccess(test_awsimple_str)  # queue that will subscribe to this topic and we'll read from at the end to test the propagation from SNS to SQS
    sqs_access.create_queue()
    sns_access = SNSAccess(test_awsimple_str)  # our test SNS topic

    sns_access.create_topic()  # this can set the permissions, which can take a while to propagate so it might fail the first time through

    # put in your actual email and run this at least once:
    # sns_access.subscribe("me@mydomain.com")

    message_string = "This is a test for awsimple."
    subject_string = "awsimple test"

    message_id = sns_access.publish(message_string, subject_string)
    print(f"{message_id=}")
    assert message_id is not None and len(message_id) > 0
