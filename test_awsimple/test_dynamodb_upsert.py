
from awsimple import DynamoDBAccess

from test_awsimple import test_awsimple_str, id_str


def test_dynamodb_upsert():
    dynamodb_access = DynamoDBAccess(profile_name=test_awsimple_str, table_name=test_awsimple_str)
    test_id = "upserter"
    dynamodb_access.delete_item(id_str, test_id)  # make sure the item doesn't exist

    item_value = {id_str: test_id, "color": "blue"}
    dynamodb_access.upsert_item(id_str, test_id, item={"color": "blue"})  # insert
    assert dynamodb_access.get_item(id_str, test_id) == item_value  # check that it's set

    item_value["size"] = 9
    dynamodb_access.upsert_item(id_str, test_id, item={"size": 9})  # update with new data
    assert dynamodb_access.get_item(id_str, test_id) == item_value  # check that it's set to the new value

    item_value["size"] = 10
    dynamodb_access.upsert_item(id_str, test_id, item={"size": 10})  # update existing data
    assert dynamodb_access.get_item(id_str, test_id) == item_value  # check that it's set to the new value
