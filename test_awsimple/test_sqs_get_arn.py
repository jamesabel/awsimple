from ismain import is_main

from awsimple import SQSAccess, is_mock

from test_awsimple import test_awsimple_str


def test_sqs_get_arn():
    sqs_access = SQSAccess(test_awsimple_str)
    sqs_access.create_queue()
    arn = sqs_access.get_arn()

    # e.g. arn:aws:sqs:us-west-2:123456789012:testawsimple
    print(f"{arn=}")
    # does not work with moto :(
    if not is_mock():
        assert arn.startswith("arn:aws:sqs:")
        # AWS region and account number is in the middle
        assert arn.endswith(f":{test_awsimple_str}")


if is_main():
    test_sqs_get_arn()
