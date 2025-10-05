import csv
from pathlib import Path
import math

from awsimple import DynamoDBAccess, dict_to_dynamodb

from test_awsimple import temp_dir

test_name = "test_dynamodb_dump_to_csv"


def test_dynamodb_dump_to_csv():

    partition_key = "id"

    input_rows = [
        {partition_key: 1, "name": "John Doe", "age": 30, "dicts_are_ignored": {"a": 2}, "lists_are_ignored": [1, 2, 3]},
        {partition_key: 2, "name": "Jane Smith", "balance": -0.5, "dicts_are_ignored": {"b": 3}, "lists_are_ignored": [4, 5, 6]},
    ]

    # create a test table and insert some data
    dynamodb_table = DynamoDBAccess(test_name)
    dynamodb_table.create_table(partition_key, partition_key_type=int)
    for input_row in input_rows:
        dynamodb_table.put_item(dict_to_dynamodb(input_row))

    output_file_path = Path(temp_dir, f"{test_name}.csv")
    dynamodb_table.dump_to_csv(output_file_path)

    # read the csv file and check the contents
    with open(output_file_path, "r") as f:
        reader = csv.DictReader(f)
        results = [row for row in reader]
        assert int(results[0][partition_key]) == 1
        assert results[0]["name"] == "John Doe"
        assert int(results[0]["age"]) == 30
        assert "dicts_are_ignored" not in results[0]
        assert "lists_are_ignored" not in results[0]
        assert int(results[1][partition_key]) == 2
        assert results[1]["name"] == "Jane Smith"
        assert math.isclose(float(results[1]["balance"]), -0.5)
        assert "dicts_are_ignored" not in results[1]
        assert "lists_are_ignored" not in results[1]
        assert len(results) == 2
