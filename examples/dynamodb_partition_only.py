import time
from pprint import pprint
from ismain import is_main

from awsimple import DynamoDBAccess


def users_example():
    """
    This example shows how to use DynamoDB to keep a table of users. This also illustrates the flexibility of NoSQL in that we can
    simply add fields at any time.

    """

    dynamodb_access = DynamoDBAccess("users_example", profile_name="testawsimple")

    # we're only using email as a partition key in our primary key (no sort key). emails are unique to each user.
    dynamodb_access.create_table("email")

    # add our first user using email, first and last name. Initially, we may think that's all we need.
    dynamodb_access.put_item({"email": "victor@victorwooten.com", "first_name": "Victor", "last_name": "Wooten"})

    # oh no. No one knows who "John Jones" is, they only know "John Paul Jones", so we need to add a middle name.
    # Luckily we are using a NoSQL database, so we just add "middle_name" in a new key/value pair. No database migration needed.
    dynamodb_access.put_item({"email": "john@ledzeppelin.com", "first_name": "John", "middle_name": "Paul", "last_name": "Jones"})

    # oh no again. No one knows who "Gordon Matthew Thomas Sumner" is either, even with 2 middle names! All they know is "Sting".
    # We need to add a nickname.  No problem since we're using a NoSQL database.
    dynamodb_access.put_item(
        {
            "email": "sting@thepolice.com",
            "first_name": "Gordon",
            "middle_name": "Matthew",
            "middle_name_2": "Thomas",
            "last_name": "Sumner",
            "nickname": "Sting",
        }
    )

    # look up user info for one of our users
    start = time.time()
    item = dynamodb_access.get_item("email", "john@ledzeppelin.com")  # this is a "get" since we're using a key and will always get back exactly one item
    end = time.time()

    pprint(item)
    print(f"took {end-start} seconds")  # should take just a fraction of a second. 0.05 seconds was a nominal value on our test system.


if is_main():
    users_example()
