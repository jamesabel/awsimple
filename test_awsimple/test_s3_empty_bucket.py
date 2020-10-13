
from awsimple import S3Access


def test_s3_empty_bucket():
    s3_access = S3Access("emptybuckettest")
    s3_access.create_bucket()
    assert len(s3_access.dir()) == 0
