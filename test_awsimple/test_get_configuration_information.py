
from awsimple import AWSAccess

from test_awsimple import test_awsimple_str


def test_get_access_key():

    access_key = AWSAccess(profile_name=test_awsimple_str).get_access_key()
    print(f"{access_key=}")
    print(f"{len(access_key)=}")
    # https://docs.aws.amazon.com/IAM/latest/APIReference/API_AccessKey.html
    assert len(access_key) >= 16  # as of this writing, the access key length was 20


def test_get_region():

    region = AWSAccess(profile_name=test_awsimple_str).get_region()
    print(f"{region=}")
    print(f"{len(region)=}")
    assert len(region) >= 5  # make sure we get back something
