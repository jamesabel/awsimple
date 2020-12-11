from ismain import is_main

from awsimple import DynamoDBAccess, QueryDirection

from test_awsimple import test_awsimple_str


def test_dynamodb_query():
    table_name = "testawsimpleps"  # ps = both partition and sort

    dynamodb_access = DynamoDBAccess(profile_name=test_awsimple_str, table_name=table_name)
    dynamodb_access.create_table("id", "name")

    # three entries for "me"
    dynamodb_access.put_item({"id": "me", "name": "james", "answer": 13})  # this will be the "first" one
    dynamodb_access.put_item({"id": "me", "name": "james abel", "answer": 1})
    dynamodb_access.put_item({"id": "me", "name": "zzz", "answer": 99})  # this will be the "last" one

    dynamodb_access.put_item({"id": "notme", "name": "notjames", "answer": 42})

    response = dynamodb_access.query("id", "me")  # partition only
    assert len(response) == 3

    response = dynamodb_access.query("id", "me", "name", "james")  # partition and sort
    assert len(response) == 1

    response = dynamodb_access.query_begins_with("id", "me", "name", "james a")  # begins with
    assert len(response) == 1
    response = dynamodb_access.query_begins_with("id", "me", "name", "jame")
    assert len(response) == 2

    response = dynamodb_access.query("id", "idonotexist")  # does not exist
    assert len(response) == 0

    response = dynamodb_access.query_one("id", "me", QueryDirection.highest)
    assert response["answer"] == 99
    assert response["name"] == "zzz"  # the "last" entry, as sorted by sort key

    response = dynamodb_access.query_one("id", "me", QueryDirection.lowest)
    assert response["answer"] == 13
    assert response["name"] == "james"  # the "first" entry, as sorted by sort key

    response = dynamodb_access.query_one("id", "idonotexist", QueryDirection.lowest)
    assert response is None


if is_main():
    test_dynamodb_query()
