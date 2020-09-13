from ismain import is_main

from awsimple import DynamoDBAccess

from test_awsimple import test_awsimple_str


def test_dynamodb_query():
    table_name = "testawsimpleps"  # ps = both partition and sort

    dynamodb_access = DynamoDBAccess(profile_name=test_awsimple_str, table_name=table_name)
    dynamodb_access.create_table("id", "name")

    dynamodb_access.put_item({"id": "me", "name": "james", "answer": 13})
    dynamodb_access.put_item({"id": "me", "name": "james abel", "answer": 1})  # two entries for "me"
    dynamodb_access.put_item({"id": "notme", "name": "notjames", "answer": 42})

    response = dynamodb_access.query("id", "me")  # partition only
    assert len(response) == 2

    response = dynamodb_access.query("id", "me", "name", "james")  # partition and sort
    assert len(response) == 1

    response = dynamodb_access.query_begins_with("id", "me", "name", "james a")  # begins with
    assert len(response) == 1
    response = dynamodb_access.query_begins_with("id", "me", "name", "jame")
    assert len(response) == 2

    response = dynamodb_access.query("id", "idonotexist")  # does not exist
    assert len(response) == 0


if is_main():
    test_dynamodb_query()
