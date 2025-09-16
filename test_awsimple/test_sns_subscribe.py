from awsimple import SNSAccess

def test_sns_subscribe():
    email = "test@abel.co"
    topic_name = "test_topic"
    sns = SNSAccess(topic_name)
    sns.create_topic()
    subscription_arn = sns.subscribe(email)  # subscribe the SQS queue to an email
    print(f"{subscription_arn=}")

def test_sns_subscribe_auto_create():
    email = "test@abel.co"
    topic_name = "test_topic_auto_create"
    sns = SNSAccess(topic_name, auto_create=True)
    subscription_arn = sns.subscribe(email)  # subscribe the SQS queue to an email
    print(f"{subscription_arn=}")