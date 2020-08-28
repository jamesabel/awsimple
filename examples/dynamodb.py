from ismain import is_main

from awsimple import DynamoDBAccess


def dynamodb():
    dynamodb_access = DynamoDBAccess(profile_name="testawsimple", table_name="testawsimple")

    # put an item into DynamoDB
    dynamodb_access.put_item({"id": "batman", "city": "Gotham"})

    # now get it back
    item = dynamodb_access.get_item("id", "batman")
    print(item["city"])  # Gotham


if is_main():
    dynamodb()
