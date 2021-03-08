from pathlib import Path
from pprint import pprint

from awsimple import DynamoDBAccess, dynamodb_to_dict
from ismain import is_main

from test_awsimple import test_awsimple_str


def test_dynamodb_sort_as_number():
    dynamodb_access = DynamoDBAccess(profile_name=test_awsimple_str, table_name=f"{test_awsimple_str}_sort_as_number", cache_dir=Path("cache"))
    dynamodb_access.create_table("id", "year", sort_key_type=int)  # sort key as number
    input_item = {"id": "me", "year": 1999, "out_of_time": False}
    dynamodb_access.put_item(input_item)
    item = dynamodb_access.get_item("id", "me", "year", 1999)
    output_item = dynamodb_to_dict(item)
    pprint(item)
    assert input_item == output_item
    dynamodb_access.delete_table()


def test_dynamodb_partition_as_number():
    dynamodb_access = DynamoDBAccess(profile_name=test_awsimple_str, table_name=f"{test_awsimple_str}_partition_as_number", cache_dir=Path("cache"))
    dynamodb_access.create_table("year", "id", partition_key_type=int)  # partition key as number
    input_item = {"id": "me", "year": 1999, "out_of_time": False}
    dynamodb_access.put_item(input_item)
    item = dynamodb_access.get_item("id", "me", "year", 1999)
    pprint(item)
    assert input_item == dynamodb_to_dict(item)

    item = dynamodb_access.query("year", 1999)[0]  # only use the partition key (no sort key)
    pprint(item)
    assert input_item == dynamodb_to_dict(item)

    dynamodb_access.delete_table()


if is_main():
    test_dynamodb_sort_as_number()
