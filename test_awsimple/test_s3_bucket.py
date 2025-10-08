import time
from logging import getLogger

import pytest
from awsimple import S3Access, BucketNotFound, __application_name__
from awsimple.platform import get_user_name, get_computer_name

from test_awsimple import test_awsimple_str

test_name = f"{__application_name__}-tests3bucket-{get_user_name()}-{get_computer_name()}".lower()  # must not contain underscores, be globally unique, and all lower case

log = getLogger(__name__)


def test_s3_bucket():
    s3_access = S3Access(test_name, profile_name=test_awsimple_str)  # use non-keyword parameter for bucket_name

    if s3_access.bucket_exists():
        s3_access.delete_bucket()
    timeout_count = 100
    while s3_access.bucket_exists() and timeout_count > 0:
        time.sleep(3)
        timeout_count -= 1

    log.info(f"{s3_access.bucket_exists()=}")

    created = s3_access.create_bucket()  # may already exist
    log.info(f"{created=}")

    # wait for bucket to exist
    timeout_count = 100
    while not s3_access.bucket_exists() and timeout_count > 0:
        time.sleep(3)
        timeout_count -= 1

    log.info(f"{s3_access.bucket_exists()=}")

    assert s3_access.bucket_exists()

    assert not s3_access.create_bucket()  # already exists
    assert s3_access.delete_bucket()

    # wait for bucket to get deleted
    timeout_count = 100
    while s3_access.bucket_exists() and timeout_count > 0:
        time.sleep(3)  # wait for bucket to exist
        timeout_count -= 1

    log.info(f"{s3_access.bucket_exists()=}")

    assert not s3_access.bucket_exists()
    assert not s3_access.delete_bucket()  # was nothing to delete


def test_s3_bucket_not_found():
    with pytest.raises(BucketNotFound):
        s3_access = S3Access("IDoNotExist")
        s3_access.dir()
