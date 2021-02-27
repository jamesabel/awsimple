from ismain import is_main

from awsimple import SNSAccess

from test_awsimple import test_awsimple_str


def test_sns_create():

    sns_access = SNSAccess(test_awsimple_str)
    sns_access.create_topic()


if is_main():
    test_sns_create()
