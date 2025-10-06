import pytest

from awsimple import DynamoDBAccess, DynamoDBItemAlreadyExists

from test_awsimple import test_awsimple_str, id_str


def test_dynamodb_if_not_exists():
    test_name = "test_dynamodb_if_not_exists"
    dynamodb_access = DynamoDBAccess(test_name, profile_name=test_awsimple_str)
    dynamodb_access.delete_table()
    dynamodb_access.create_table(id_str, partition_key_type=int)

    test_data = {id_str: 1, "value": 100}

    dynamodb_access.put_item(test_data)

    with pytest.raises(DynamoDBItemAlreadyExists):
        dynamodb_access.put_item_if_not_exists({id_str: 1, "value": 200})


def test_dynamodb_if_not_exists_sort_key():

    test_name = "test_dynamodb_if_not_exists_sort_key"

    sort_key = "value"

    dynamodb_access = DynamoDBAccess(test_name, profile_name=test_awsimple_str)
    dynamodb_access.delete_table()
    dynamodb_access.create_table(id_str, sort_key, partition_key_type=int, sort_key_type=int)

    test_value = 100

    test_data = {id_str: 1, sort_key: test_value}

    dynamodb_access.put_item_if_not_exists(test_data)

    with pytest.raises(DynamoDBItemAlreadyExists):
        dynamodb_access.put_item_if_not_exists({id_str: 1, sort_key: test_value})


def test_dynamodb_if_not_exists_sort_key_different():

    test_name = "test_dynamodb_if_not_exists_sort_key_different"

    sort_key = "value"

    dynamodb_access = DynamoDBAccess(test_name, profile_name=test_awsimple_str)
    dynamodb_access.delete_table()
    dynamodb_access.create_table(id_str, sort_key, partition_key_type=int, sort_key_type=int)

    test_value = 100

    test_data = {id_str: 1, sort_key: test_value}

    dynamodb_access.put_item_if_not_exists(test_data)
    dynamodb_access.put_item_if_not_exists({id_str: 1, sort_key: 200})  # different sort key, should work
