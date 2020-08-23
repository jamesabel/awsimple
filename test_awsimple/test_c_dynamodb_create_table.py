import time

from awsimple import DynamoDBAccess
from test_awsimple import test_awsimple_str


def test_dynamodb_create_table():
    table_name = f"{test_awsimple_str}temp"

    dynamodb_access = DynamoDBAccess(profile_name=test_awsimple_str, table_name=table_name)

    dynamodb_access.create_table("id")
    time.sleep(10)
    timeout_count = 10
    while not dynamodb_access.table_exists() and timeout_count > 0:
        print("waiting for table to be created")
        time.sleep(10)
        timeout_count -= 1
    assert dynamodb_access.table_exists()

    dynamodb_access.delete_table()
    time.sleep(10)
    timeout_count = 10
    while dynamodb_access.table_exists() and timeout_count > 0:
        print("waiting for table to be deleted")
        time.sleep(10)
        timeout_count -= 1
    assert not dynamodb_access.table_exists()

    assert not dynamodb_access.delete_table()  # test calling delete_table() on a non-existent table
