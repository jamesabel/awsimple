import pytest

from botocore.exceptions import ProfileNotFound

from awsimple import AWSAccess, S3Access, DynamoDBAccess, SQSAccess, is_mock

from test_awsimple import test_awsimple_str


def test_aws_test():
    # test the test() method (basic AWS connection)

    # these should work
    if not is_mock():
        assert AWSAccess(profile_name=test_awsimple_str).test()
    assert S3Access(test_awsimple_str, profile_name=test_awsimple_str).test()
    assert DynamoDBAccess(test_awsimple_str, profile_name=test_awsimple_str).test()
    assert SQSAccess(test_awsimple_str, profile_name=test_awsimple_str).test()

    if not is_mock():
        # this (non-existent) profile doesn't have access at all
        with pytest.raises(ProfileNotFound):
            AWSAccess(profile_name="IAmNotAProfile").test()
