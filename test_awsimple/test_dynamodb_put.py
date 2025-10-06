from awsimple import DynamoDBAccess

from test_awsimple import test_awsimple_str, id_str

test_name = "test_dynamodb_put"


def test_dynamodb_put():
    dynamodb_access = DynamoDBAccess(test_name, profile_name=test_awsimple_str)
    dynamodb_access.create_table(id_str, partition_key_type=int)

    test_data = {id_str: 1, "name": test_name, "value": 100}

    dynamodb_access.put_item(test_data)

    assert dynamodb_access.get_item(id_str, test_data[id_str]) == test_data
