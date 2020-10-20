import platform
import getpass

from awsimple import S3Access


def test_s3_empty_bucket():
    bucket_name = f"emptybuckettest{platform.node()}{getpass.getuser()}"  # must be globally unique when using real S3
    print(f"{bucket_name=}")
    s3_access = S3Access(bucket_name)
    s3_access.create_bucket()
    assert len(s3_access.dir()) == 0
