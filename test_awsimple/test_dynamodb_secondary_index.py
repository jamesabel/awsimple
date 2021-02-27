from awsimple import DynamoDBAccess
from copy import deepcopy

from test_awsimple import test_awsimple_str, id_str


def test_dynamodb_secondary_index():

    table_name = f"{test_awsimple_str}2"
    table = DynamoDBAccess(table_name)

    sort_key = "id2"
    secondary_index = "id3"
    table.create_table(id_str, sort_key, secondary_index)

    item = {id_str: "me", sort_key: "myself", secondary_index: "i"}
    table.put_item(item)

    item2 = deepcopy(item)
    item2[sort_key] = "moi même"  # also test unicode!
    item2[secondary_index] = "je"
    table.put_item(item2)

    query_results = table.query(id_str, "me")
    print(f"{query_results=}")
    assert len(query_results) == 2  # just the partition key should provide us with both rows

    assert table.query(secondary_index, "je") == [item2]  # with (only) the secondary index (in DynamoDB you can't mix primary and secondary indexes)

    table.delete_table()
