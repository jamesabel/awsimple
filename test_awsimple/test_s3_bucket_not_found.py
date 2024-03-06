import pytest

from awsimple import S3Access, BucketNotFound

from test_awsimple import test_awsimple_str


def test_s3_bucket_not_found():
    s3_access = S3Access(profile_name=test_awsimple_str, bucket_name="doesnotexist")
    with pytest.raises(BucketNotFound):
        s3_access.keys()
