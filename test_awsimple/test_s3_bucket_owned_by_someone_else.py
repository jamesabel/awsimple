import pytest

from awsimple import S3Access, is_mock, S3BucketAlreadyExistsNotOwnedByYou


def test_s3_bucket_owned_by_someone_else():
    if not is_mock():
        s3 = S3Access("google")
        with pytest.raises(S3BucketAlreadyExistsNotOwnedByYou):
            s3.create_bucket()
