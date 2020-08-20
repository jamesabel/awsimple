import time

from awsimple import S3Access
from test_awsimple import test_awsimple_str


def test_s3():

    test_string = str(time.time())  # so it changes between tests

    s3_access = S3Access(profile_name=test_awsimple_str, bucket=test_awsimple_str)
    assert s3_access.create_bucket()
    s3_access.write_string(test_string, test_awsimple_str)
    assert s3_access.read_string(test_awsimple_str) == test_string
