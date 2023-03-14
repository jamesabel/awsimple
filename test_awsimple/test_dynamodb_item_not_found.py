import pytest

from awsimple import DynamoDBAccess, DBItemNotFound

from test_awsimple import test_awsimple_str, id_str


def test_dynamodb_item_not_found():
    dynamodb_access = DynamoDBAccess(test_awsimple_str, profile_name=test_awsimple_str)
    dynamodb_access.create_table(id_str)
    with pytest.raises(DBItemNotFound):
        dynamodb_access.get_item(id_str, "I will never ever exist")
