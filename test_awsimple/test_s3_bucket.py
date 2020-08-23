import time

from awsimple import S3Access

from test_awsimple import test_awsimple_str

test_bucket_name = f"{test_awsimple_str}temp"  # temp bucket that will be created and deleted


def test_s3_bucket():
    s3_access = S3Access(profile_name=test_awsimple_str, bucket=test_bucket_name)
    s3_access.create_bucket()  # may already exist

    timeout_count = 100
    while not s3_access.bucket_exists() and timeout_count > 0:
        time.sleep(3)  # wait for bucket to exist
        timeout_count -= 1
    assert s3_access.bucket_exists()

    assert not s3_access.create_bucket()  # already exists
    assert s3_access.delete_bucket()
    assert not s3_access.delete_bucket()  # was nothing to delete
    assert not s3_access.bucket_exists()