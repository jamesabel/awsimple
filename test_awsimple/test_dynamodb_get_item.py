from awsimple import DynamoDBAccess

from test_awsimple import test_awsimple_str, id_str


def test_dynamodb_get_item():
    test_id = "test_id"
    dynamodb_access = DynamoDBAccess(profile_name=test_awsimple_str, table_name=test_awsimple_str)
    dynamodb_access.create_table(id_str)
    dynamodb_access.delete_item(id_str, test_id)  # make sure the item doesn't exist

    item_value = {id_str: test_id, "color": "blue"}
    dynamodb_access.upsert_item(id_str, test_id, item={"color": "blue"})  # insert
    assert dynamodb_access.get_item(id_str, test_id) == item_value  # check that it's set
    assert dynamodb_access.get_item(partition_value=test_id) == item_value  # check that it's set
