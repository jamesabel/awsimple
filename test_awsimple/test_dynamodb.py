import os
import sys
import decimal
from collections import OrderedDict, defaultdict
import math
import datetime
from datetime import timedelta
import pickle

from PIL import Image
from ismain import is_main

from awsimple import dict_to_dynamodb, DynamoDBAccess
from test_awsimple import dict_is_close, test_awsimple_str, id_str

dict_id = "test"

# source:
# https://en.wikipedia.org/wiki/Portable_Network_Graphics
# https://en.wikipedia.org/wiki/File:PNG_transparency_demonstration_1.png
png_image = Image.open(os.path.join("test_awsimple", "280px-PNG_transparency_demonstration_1.png"))

od = OrderedDict()
od["a"] = 1
od["b"] = 2

dd = defaultdict(int)
dd[1] = 2

sample_input = {
    id_str: dict_id,
    "sample1": "Test Data",
    "sample2": 2.0,
    "sample3": True,
    "sample4": int(1),
    "sample5": None,
    "sample6": {"test": True},
    "sample7": ["Hello", "World"],
    "sample8": [9, 10],
    "od": od,
    "dd": dd,
    "DecimalInt": decimal.Decimal(42),
    "DecimalFloat": decimal.Decimal(2.0) / decimal.Decimal(3.0),
    "a_tuple": (1, 2, 3),
    42: "my_key_is_an_int",
    "difficult_floats": [math.pi, math.e, 0.6],
    "difficult_ints": [sys.maxsize],
    "image": png_image,
    "test_date_time": datetime.datetime.fromtimestamp(1559679535),  # 2019-06-04T13:18:55
    "zero_len_string": "",
}


def check_table_contents(contents):
    with open(os.path.join("cache", f"{test_awsimple_str}.pickle"), "rb") as f:
        assert dict_is_close(sample_input, contents[0])
        assert dict_is_close(sample_input, pickle.load(f)[0])


def test_dynamodb():

    dynamodb_dict = dict_to_dynamodb(sample_input)

    assert dynamodb_dict["sample1"] == "Test Data"
    assert math.isclose(float(dynamodb_dict["sample2"]), decimal.Decimal(2.0))
    assert dynamodb_dict["sample3"] is True
    assert dynamodb_dict["sample5"] is None
    assert dynamodb_dict["sample6"] == {"test": True}
    assert dynamodb_dict["sample7"] == ["Hello", "World"]
    assert dynamodb_dict["sample8"] == [decimal.Decimal(9), decimal.Decimal(10)]
    assert dynamodb_dict["DecimalInt"] == decimal.Decimal(42)
    assert dynamodb_dict["DecimalFloat"] == decimal.Decimal(2.0) / decimal.Decimal(3.0)
    assert dynamodb_dict["a_tuple"] == [1, 2, 3]
    assert dynamodb_dict["42"] == "my_key_is_an_int"  # test conversion of an int key to a string
    assert dynamodb_dict["test_date_time"] == "2019-06-04T13:18:55"
    assert dynamodb_dict["zero_len_string"] is None

    dynamodb_access = DynamoDBAccess(profile_name=test_awsimple_str, table_name=test_awsimple_str, cache_life=timedelta(seconds=1).total_seconds())
    dynamodb_access.create_table("id")
    dynamodb_access.put_item(dynamodb_dict)

    sample_from_db = dynamodb_access.get_item(id_str, dict_id)
    assert sample_from_db == dynamodb_dict  # make sure we get back exactly what we wrote

    table_contents = dynamodb_access.scan_table_cached()
    check_table_contents(table_contents)
    table_contents = dynamodb_access.scan_table()
    check_table_contents(table_contents)
    table_contents = dynamodb_access.scan_table_cached()
    check_table_contents(table_contents)

    dynamodb_tables = dynamodb_access.get_table_names()
    print(dynamodb_tables)
    assert len(dynamodb_tables) > 0
    assert test_awsimple_str in dynamodb_tables


if is_main():
    test_dynamodb()
