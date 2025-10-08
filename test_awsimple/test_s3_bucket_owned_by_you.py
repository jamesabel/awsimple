from awsimple import S3Access
from awsimple.platform import get_user_name, get_computer_name

test_name = f"test-s3-bucket-owned-by-you-{get_user_name()}-{get_computer_name()}".lower()  # bucket names must be lowercase and no underscores


def test_s3_bucket_owned_by_you():
    s3 = S3Access(test_name)
    s3.create_bucket()  # ensure created
    created = s3.create_bucket()  # already created
    assert not created
