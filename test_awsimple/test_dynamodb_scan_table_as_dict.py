from pathlib import Path
from datetime import timedelta
from decimal import Decimal

from awsimple import DynamoDBAccess
from test_awsimple import test_awsimple_str, id_str


def test_dynamodb_scan_table_as_dict():

    dynamodb_access = DynamoDBAccess(profile_name=test_awsimple_str, table_name=test_awsimple_str, cache_dir=Path("cache"), cache_life=timedelta(seconds=10).total_seconds())
    dynamodb_access.create_table(id_str)
    dynamodb_access.put_item({id_str: "b", "value": 1})  # will be sorted in a different order than we're inputting
    dynamodb_access.put_item({id_str: "c", "value": 3})
    dynamodb_access.put_item({id_str: "a", "value": 2})

    expected_contents = {"a": {"id": "a", "value": Decimal("2")}, "b": {"id": "b", "value": Decimal("1")}, "c": {"id": "c", "value": Decimal("3")}}
    table_contents = dynamodb_access.scan_table_as_dict()
    assert list(table_contents.keys()) == ["a", "b", "c"]  # check sort
    assert table_contents == expected_contents

    table_contents = dynamodb_access.scan_table_cached_as_dict()
    assert list(table_contents.keys()) == ["a", "b", "c"]  # check sort
    assert table_contents == expected_contents

    table_contents = dynamodb_access.scan_table_cached_as_dict()
    assert dynamodb_access.cache_hit
    assert list(table_contents.keys()) == ["a", "b", "c"]  # check sort
    assert table_contents == expected_contents

    table_contents = dynamodb_access.scan_table_cached_as_dict(sort_key=lambda x: x[id_str])  # test sort_key
    assert table_contents == expected_contents
