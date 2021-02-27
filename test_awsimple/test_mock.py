from awsimple import is_mock, S3Access

from test_awsimple import test_awsimple_str


def test_mock():
    s3_access = S3Access(test_awsimple_str)
    assert is_mock() == s3_access.is_mocked()  # make sure that the AWSAccess instance is actually using mocking
