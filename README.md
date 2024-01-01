<p align="center">
    <!--
    <a href="https://app.circleci.com/pipelines/github/jamesabel/awsimple" alt="build">
        <img src="https://img.shields.io/circleci/build/gh/jamesabel/awsimple" />
    </a>
    -->
    <a href="https://codecov.io/gh/jamesabel/awsimple" alt="codecov">
        <img src="https://img.shields.io/codecov/c/github/jamesabel/awsimple/master" />
    </a>
    <a href="https://pypi.org/project/awsimple/" alt="pypi">
        <img src="https://img.shields.io/pypi/v/awsimple" />
    </a>
    <a href="https://pypi.org/project/awsimple/" alt="downloads">
        <img src="https://img.shields.io/pypi/dm/awsimple" />
    </a>
    <!--
    <a alt="python">
        <img src="https://img.shields.io/pypi/pyversions/awsimple" />
    </a>
    -->
    <a alt="license">
        <img src="https://img.shields.io/github/license/jamesabel/awsimple" />
    </a>
</p>

# AWSimple

*(pronounced A-W-Simple)*

Simple API for basic AWS services such as S3 (Simple Storage Service), DynamoDB (a NoSQL database), SNS (Simple Notification Service), 
and SQS (Simple Queuing Service).

Project featured on [PythonBytes Podcast Episode #224](https://pythonbytes.fm/episodes/show/224/join-us-on-a-python-adventure-back-to-1977).

Full documentation available on [Read the Docs](https://awsimple.readthedocs.io/) .

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

- Supports moto mock and localstack. Handy for testing and CI.


## Usage

    pip install awsimple

## Examples

The example folder has several examples you can customize and run. Instructions are available in [examples](EXAMPLES.md)

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

DynamoDB cached table scans are particularly useful for tables that are infrequently updated.

## What`awsimple` Is Not

- `awsimple` is not necessarily the most memory and CPU efficient

- `awsimple` does not provide cost monitoring hooks

- `awsimple` does not provide all the options and features that the regular AWS API (e.g. boto3) does

## Updates/Releases

3.x.x - Cache life for cached DynamoDB scans is now based on most recent table modification time (kept in a separate 
table). Explict cache life is no longer required (parameter has been removed).

## Testing using moto mock and localstack

moto mock-ing can improve performance and reduce AWS costs.  `awsimple` supports both moto mock and localstack.
In general, it's recommended to develop with mock and finally test with the real AWS services.

Select via environment variables:

  - AWSIMPLE_USE_MOTO_MOCK=1  # use moto
  - AWSIMPLE_USE_LOCALSTACK=1  # use localstack

### Test Time

| Method     | Test Time (seconds) | Speedup (or slowdown) | Comment         |
|------------|---------------------|-----------------------|-----------------|
| AWS        | 462.65              | 1x                    | baseline        |
| mock       | 40.46               | 11x                   | faster than AWS |
| localstack | 2246.82             | 0.2x                  | slower than AWS |

System: Intel&reg; Core&trade; i7 CPU @ 3.47GHz, 32 GB RAM

## Contributing

Contributions are welcome, and more information is available in the [contributing guide](CONTRIBUTING.md).