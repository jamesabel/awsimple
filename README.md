<p align="center">
    <a href="https://github.com/jamesabel/awsimple/actions/workflows/python-package.yml">
        <img src="https://img.shields.io/github/actions/workflow/status/jamesabel/awsimple/python-package.yml?branch=main&label=tests" alt="tests" />
    </a>
    <a href="https://codecov.io/gh/jamesabel/awsimple">
        <img src="https://img.shields.io/codecov/c/github/jamesabel/awsimple/main" alt="coverage" />
    </a>
    <a href="https://jamesabel.github.io/awsimple/">
        <img src="https://img.shields.io/github/actions/workflow/status/jamesabel/awsimple/docs.yml?branch=main&label=docs" alt="documentation" />
    </a>
    <a href="https://pypi.org/project/awsimple/">
        <img src="https://img.shields.io/pypi/v/awsimple" alt="PyPI version" />
    </a>
    <a href="https://pypi.org/project/awsimple/">
        <img src="https://img.shields.io/pypi/pyversions/awsimple" alt="Python versions" />
    </a>
    <a href="https://pypi.org/project/awsimple/">
        <img src="https://img.shields.io/pypi/dm/awsimple" alt="downloads per month" />
    </a>
    <a href="https://github.com/jamesabel/awsimple/blob/main/LICENSE">
        <img src="https://img.shields.io/github/license/jamesabel/awsimple" alt="license" />
    </a>
    <a href="https://pypi.org/project/awsimple/">
        <img src="https://img.shields.io/pypi/types/awsimple" alt="typed" />
    </a>
    <a href="https://github.com/astral-sh/ruff">
        <img src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json" alt="code style: ruff" />
    </a>
</p>

# AWSimple

*(pronounced A-W-Simple)*

Simple API for basic AWS services such as S3 (Simple Storage Service), DynamoDB (a NoSQL database), SNS (Simple Notification Service), 
and SQS (Simple Queuing Service).

Project featured on [PythonBytes Podcast Episode #224](https://pythonbytes.fm/episodes/show/224/join-us-on-a-python-adventure-back-to-1977).

Full documentation available at [jamesabel.github.io/awsimple](https://jamesabel.github.io/awsimple/).

### Features:

- Simple Object-Oriented API on top of boto3.
  - Eliminates the need to worry about `clients`, `resources`, `sessions`, and pagination.

- Locally cached S3 accesses. Reduces network traffic, AWS costs, and can speed up access.

- `pubsub` functionality (via SNS topics and SQS queues).

- DynamoDB full table scans (with local cache option that only rescans if the table has changed).

- Convert back and forth between DynamoDB items and Python dictionaries automatically. Converts many common data types to DynamoDB compatible types,
including nested structures, sets, images (PIL), and Enum/StrEnum.

- Full-object checksums (CRC64NVME) for S3 files (S3's etag is not a true file hash).

- Supports moto mock and localstack. Handy for testing and CI.

- Automatic S3 retries.

- One-line S3 file write, read, and delete.

- DynamoDB secondary indexes.

- Built-in pagination (e.g. for DynamoDB table scans and queries).  Always get everything you asked for.

- Can automatically set SQS timeouts based on runtime data (can also be user-specified).

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

`awsimple` uses S3's native full-object checksums to test for file equivalency (e.g. to avoid re-uploading
unchanged files). Uploads use CRC64NVME - the algorithm S3 itself defaults to, and the only one S3 computes
as a full-object value for both single-part and multipart uploads - and S3 validates the data against the
checksum server-side before storing. Objects written by other tools with a native full-object SHA-512
checksum are also recognized and compared. Objects without any recognizable full-object hash fall back to
modification-time and file-size comparison.

An S3 object has one native checksum, so `awsimple` also writes the SHA-512 of the contents as object metadata
(`awsimple-sha512`) on every object it writes. A matching CRC64NVME alone does not skip an upload: the length and
the SHA-512 (when the object has one) must match too. An object that has the metadata SHA-512 but no native
checksum (e.g. written by an older awsimple version) is always re-uploaded, so it gains the native checksum.

Every S3 access checks the length and the CRC64NVME (when the object has one) of what was transferred: after a
write, the S3 object is compared with what was written, and after a read or download, what was received is compared
with the S3 object. A mismatch raises `AWSimpleException` (a download is tried again first).

`S3Access.list_objects()` lists a bucket (or a prefix) with the size, modification time, ETag and storage class of
each object, using only the bucket listing: it makes no request per object, unlike `S3Access.dir()`.

When the destination of a download is a directory, the S3 key becomes a path under that directory with
`s3_key_to_local_path()`: empty key components are dropped (as `aws s3 sync` does), so a key such as `/temp//a.txt`
is `temp/a.txt` under the directory, and a key that can not be a file under the directory (e.g. one with a `..`
component) raises `AWSimpleException`. `S3Access.download()` also takes the object's metadata when the caller already
has it, and a callback that reports the bytes transferred.

`S3Access.sync_to_directory()` brings a local directory up to date with a bucket (or a prefix), like `aws s3 sync` from
a bucket to a directory, without the AWS CLI:

    result = S3Access(profile_name="testawsimple", bucket_name="testawsimple").sync_to_directory(Path("backup"))
    print(result.get_count(S3SyncOutcome.downloaded), result.get_size(S3SyncOutcome.downloaded))
    for sync_object in result.get_objects(S3SyncOutcome.failed):
        print(sync_object.key, sync_object.reason)

The bucket is listed once, and an object is downloaded when its local file is missing, has a different size, or is
older than the S3 object (`size_only=True` does not compare times). Only a downloaded object takes a metadata request
(one), which the length and CRC64NVME of what was downloaded are checked against before it replaces the local file.
Local files are never deleted. The prefix is not part of the local paths. The result has the outcome of every listed
object: folder markers (keys that end with `/`), `GLACIER` and `DEEP_ARCHIVE` objects, and keys that can not all be
local files (keys that are the same local file, e.g. `a/b` and `a//b`, or a key that is a file where other keys need
a directory) are skipped with the reason, and a failure of one object is reported without stopping the others. There
is a dry run, an event to request a stop, and a callback per object.

## Caching

S3 objects and DynamoDB tables can be cached locally to reduce network traffic, minimize AWS costs, 
and potentially offer a speedup.

DynamoDB cached table scans are particularly useful for tables that are infrequently updated.

The S3 cache is based on the SHA-512 of an object's contents: cached contents are in a file named with their SHA-512.
When an S3 object has a SHA-512 (S3's native full-object SHA-512, or the `awsimple-sha512` object metadata), that is
used to find it in the cache. S3 does not validate object metadata, so downloaded contents are only cached once their
calculated SHA-512 matches. `awsimple` never updates an S3 object's metadata on a read. When an S3 object has no
SHA-512, it is instead checked against its native CRC64NVME, key, size and modification time: a SHA-512 of these
(with the bucket, version and ETag) is mapped by a local index to the SHA-512 calculated when the contents were
downloaded. The key is only hashed, never used as a cache file name, so file system path irregularities do not
matter. A CRC alone is never a cache key, since a CRC is not collision resistant.

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