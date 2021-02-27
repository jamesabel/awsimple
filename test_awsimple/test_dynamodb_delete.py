import pytest

from awsimple import DynamoDBAccess, DBItemNotFound

from test_awsimple import test_awsimple_str, id_str


def test_dynamodb_delete():
    dynamodb_access = DynamoDBAccess(profile_name=test_awsimple_str, table_name=test_awsimple_str)
    dynamodb_access.create_table(id_str)
    test_id = "deleter"
    item_value = {id_str: test_id, "color": "blue"}
    dynamodb_access.put_item(item_value)
    assert dynamodb_access.get_item(id_str, test_id) == item_value  # check that it's set
    dynamodb_access.delete_item(id_str, test_id)
    with pytest.raises(DBItemNotFound):
        print(dynamodb_access.get_item(id_str, test_id))  # check that it's deleted
