import pytest

from awsimple import DynamoDBAccess, DynamoDBTableNotFound

from test_awsimple import test_awsimple_str


def test_dynamodb_table_not_found_put_item():
    with pytest.raises(DynamoDBTableNotFound):
        dynamodb_access = DynamoDBAccess("does_not_exist", profile_name=test_awsimple_str)
        dynamodb_access.put_item(item={})  # table won't exist


def test_dynamodb_table_not_found_upsert_item():
    with pytest.raises(DynamoDBTableNotFound):
        dynamodb_access = DynamoDBAccess("does_not_exist", profile_name=test_awsimple_str)
        dynamodb_access.upsert_item(item={})  # table won't exist


def test_dynamodb_table_not_found_get_item():
    with pytest.raises(DynamoDBTableNotFound):
        dynamodb_access = DynamoDBAccess("does_not_exist", profile_name=test_awsimple_str)
        dynamodb_access.get_item("dummy", "dummy")  # table won't exist
