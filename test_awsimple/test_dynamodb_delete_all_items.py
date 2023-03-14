import time

from awsimple import dict_to_dynamodb, DynamoDBAccess, is_mock

from test_awsimple import id_str, test_awsimple_str


def test_dynamodb_delete_all_items():
    table_name = "awsimple-delete-test"  # this test is the only thing we'll use this table for

    dynamodb_access = DynamoDBAccess(table_name, profile_name=test_awsimple_str)
    dynamodb_access.create_table(id_str)
    dynamodb_access.put_item(dict_to_dynamodb({id_str: "me", "answer": 42}))
    dynamodb_access.put_item(dict_to_dynamodb({id_str: "you", "question": 0}))
    while len(table_contents := dynamodb_access.scan_table()) != 2:
        print(f"waiting for the put ...{table_contents}")
        time.sleep(1)  # DynamoDB is "eventually consistent"
    rows_deleted = dynamodb_access.delete_all_items()
    assert rows_deleted == 2
    while len(table_contents := dynamodb_access.scan_table()) != 0:
        print(f"waiting for the delete all items ...{table_contents}")
        time.sleep(1)  # DynamoDB is "eventually consistent"
