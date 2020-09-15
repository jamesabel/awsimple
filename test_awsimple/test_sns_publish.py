import json

from ismain import is_main

from awsimple import SNSAccess, SQSPollAccess, SQSAccess

from test_awsimple import test_awsimple_str


def test_sns_publish():

    SQSAccess(test_awsimple_str).receive_messages()  # drain the queue

    sqs_access = SQSPollAccess(test_awsimple_str)  # queue that will subscribe to this topic and we'll read from at the end to test the propagation from SNS to SQS
    sns_access = SNSAccess(test_awsimple_str)  # our test SNS topic

    sns_access.create_topic()  # this can set the permissions, which can take a while to propagate so it might fail the first time through

    subscription_arn = sns_access.subscribe(sqs_access)  # subscribe the SQS queue to the SNS topic
    print(f"{subscription_arn=}")

    # put in your actual email and run this at least once:
    # sns_access.subscribe("me@mydomain.com")

    message_string = "There's a new package on PyPI called awsimple. Check it out."
    subject_string = "simple AWS access"
    sns_access.publish(message_string, subject_string)
    message = json.loads(sqs_access.receive_message().message)
    returned_message_string = message["Message"]
    print(returned_message_string)
    assert returned_message_string == message_string


if is_main():
    test_sns_publish()
