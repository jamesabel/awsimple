# awsimple

Simple API for basic AWS services such as S3 (Simple Storage Service), DynamoDB (a NoSQL database), SNS (Simple Notification Service), 
and SQS (Simple Queuing Service).

### Features:

- Simple Object Oriented API on top of boto3

- One-line S3 file write, read, and delete

- Automatic S3 retries

- Locally cached S3 accesses

- True file hashing (SHA512) for S3 files (S3's etag is not a true file hash)

- DynamoDB full table scans (with local cache option)

- DynamoDB secondary indexes

- Built-in pagination (e.g. for DynamoDB table scans and queries).  Always get everything you asked for.

- Can automatically set SQS timeouts based on runtime data (can also be user-specified)

## Usage

    pip install awsimple

## Examples

### S3

    # print string contents of an existing S3 object
    s = S3Access(profile_name="testawsimple", bucket="testawsimple").read_string("helloworld.txt")
    print(s)

### DynamoDB

    dynamodb_access = DynamoDBAccess(profile_name="testawsimple", table_name="testawsimple")

    # put an item into DynamoDB
    dynamodb_access.put_item({"id": "batman", "city": "Gotham"})

    # now get it back
    item = dynamodb_access.get_item("id", "batman")
    print(item["city"])  # Gotham

## Introduction

`awsimple` is a simple interface into basic AWS services such as S3 (Simple Storage Service) and
DynamoDB (a simple NoSQL database).  It has a set of higher level default settings and behavior
that should cover many basic usage models.

## Discussion

AWS's "serverless" resources offer many benefits.  You only pay for what you use, easily scale, 
and generally have high performance and availability.

While AWS has many varied services with extensive flexibility, using it for more straight-forward 
applications is sometimes a daunting task. There are access modes that are probably not requried 
and some default behaviors are not best for common usages.  `awsimple` aims to create a higher 
level API to AWS services (such as S3, DynamoDB, SNS, and SQS) to improve programmer productivity.


## S3

`awsimple` calculates the local file hash (sha512) and inserts it into the S3 object metadata.  This is used
to test for file equivalency.

## Caching

S3 objects and DynamoDB tables can be cached locally to reduce network traffic, minimize AWS costs, 
and potentially offer a speedup.

## What`awsimple` Is Not

- `awsimple` is not necessarily the most memory and CPU efficient

- `awsimple` does not provide cost monitoring hooks

- `awsimple` does not provide all the options and features that the regular AWS API (e.g. boto3) does
