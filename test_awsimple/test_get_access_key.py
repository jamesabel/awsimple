
from awsimple import DynamoDBAccess

from test_awsimple import test_awsimple_str


def test_dynamodb_item_not_found():

    dynamodb_access = DynamoDBAccess(test_awsimple_str, profile_name=test_awsimple_str)
    access_key = dynamodb_access.get_access_key()
    print(f"{access_key=}")
    print(f"{len(access_key)=}")
    # https://docs.aws.amazon.com/IAM/latest/APIReference/API_AccessKey.html
    assert len(access_key) >= 16  # as of this writing, the access key length was 20
