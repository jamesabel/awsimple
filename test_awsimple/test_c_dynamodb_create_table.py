from pprint import pprint

from awsimple import DynamoDBAccess
from test_awsimple import test_awsimple_str


def test_dynamodb_create_table():
    table_name = f"{test_awsimple_str}temp"

    dynamodb_access = DynamoDBAccess(profile_name=test_awsimple_str, table_name=table_name)

    dynamodb_access.create_table("id")
    assert dynamodb_access.table_exists()  # create_table has a waiter so the table should exist at this point

    dynamodb_access.put_item({"id": "me", "value": 1})

    table_data = dynamodb_access.scan_table_cached()
    pprint(table_data)
    assert table_data[0]["id"] == "me"
    assert table_data[0]["value"] == 1
    assert len(table_data) == 1
    assert len(dynamodb_access.scan_table_cached(invalidate_cache=True)) == 1

    dynamodb_access.delete_table()
    assert not dynamodb_access.delete_table()  # delete_table has a waiter so the table should exist at this point
