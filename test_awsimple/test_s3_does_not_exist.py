import pytest
from awsimple import S3Access, AWSimpleException

from test_awsimple import test_awsimple_str


def test_s3_object_does_not_exist():
    i_do_not_exist_key = "i-do-not-exist"

    s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)  # keyword parameter for bucket_name
    assert s3_access.bucket_exists()  # make sure the bucket exists
    with pytest.raises(s3_access.client.exceptions.NoSuchKey):
        s3_access.read_string(i_do_not_exist_key)

    with pytest.raises(AWSimpleException):
        s3_access.get_s3_object_metadata(i_do_not_exist_key)
