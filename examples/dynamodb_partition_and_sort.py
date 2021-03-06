import time
from pprint import pprint

from ismain import is_main

from awsimple import DynamoDBAccess, dict_to_dynamodb


def musical_instruments_example():

    """
    This example shows how to use DynamoDB to keep a table of musical instruments.

    """

    dynamodb_access = DynamoDBAccess("musical_instruments_example", profile_name="testawsimple")

    # our primary key is a composite of partition (manufacturer) and sort (serial_number).
    # for a particular manufacturer, serial numbers define exactly one instrument
    dynamodb_access.create_table("manufacturer", "serial_number")

    # we have to convert float to a Decimal for DynamoDB
    dynamodb_access.put_item(dict_to_dynamodb({"manufacturer": "Gibson", "serial_number": "1234", "model": "Ripper", "year": 1983, "price": 1250.0}))

    dynamodb_access.put_item(dict_to_dynamodb({"manufacturer": "Gibson", "serial_number": "5678", "model": "Thunderbird", "year": 1977, "price": 2400.0}))

    dynamodb_access.put_item(
        dict_to_dynamodb(
            {
                "manufacturer": "Fender",
                "serial_number": "1234",
                "model": "Precision",
                "year": 2008,
                "price": 1800.0,
            }  # same serial number as the Gibson Ripper, but that's OK since this is Fender
        )
    )

    # get all the Gibson instruments
    start = time.time()
    item = dynamodb_access.query("manufacturer", "Gibson")  # this can (and will in this case) be multiple items
    end = time.time()
    pprint(item)
    print(f"query took {end-start} seconds")  # nominal 0.1 to 0.15 seconds
    print()

    # get the entire inventory
    start = time.time()
    all_items = dynamodb_access.scan_table_cached()  # use cached if the table is large and *only* if we know our table is slowly or never changing
    end = time.time()
    pprint(all_items)
    print(f"scan took {end-start} seconds ({dynamodb_access.cache_hit=})")  # always fast for this small data set, but caching can offer a speedup for large tables


if is_main():
    musical_instruments_example()
