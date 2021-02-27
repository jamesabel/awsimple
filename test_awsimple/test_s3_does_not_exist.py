import pytest
from awsimple import S3Access

from test_awsimple import test_awsimple_str


def test_s3_object_does_not_exist():
    s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)  # keyword parameter for bucket_name
    assert s3_access.bucket_exists()  # make sure the bucket exists
    with pytest.raises(s3_access.client.exceptions.NoSuchKey):
        s3_access.read_string("i-do-not-exist")
