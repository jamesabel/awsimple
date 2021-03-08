
AWSimple User Guide
===================

AWSimple provides a high level and object oriented access to common AWS "serverless" services such as
:ref:`S3`, :ref:`DynamoDB`, :ref:`SNS`, and :ref:`SQS`. AWSimple uses AWS'
`boto3 <https://boto3.amazonaws.com/v1/documentation/api/latest/index.html>`_ "under the hood" for AWS access.

Setting up your AWS Account
---------------------------
In order to use AWSimple, or any other AWS software for that matter, you need an AWS account and one or more AWS "programmatic users" created via the
`AWS IAM (Identity and Access Management) console <https://aws.amazon.com/iam/>`_. This user guide assumes you have a basic understanding of the AWS IAM.
This programmatic user will need to be given appropriate permissions to the AWS resources you wish to use.  IAM provides you with an `access key` and
`secret access key` for a programmatic user. You must also select an AWS `region` (i.e. roughly where the actual AWS servers that you'll be using
are located). These keys must be provided to AWSimple in order to access AWS resources.

IMHO, at least for the purposes of initial development, you probably don't have to worry too much about fine-tuning your region. Pick a region reasonably
close and go with that for a while. AWS's global network is pretty good, so just get close at first and you can optimize later. Many permissions and/or
access issues can arise when you inadvertently try to access an unintended region.

During development, it is recommended that these keys be placed in the AWS `credentials` and `config` files (no file extension) in the `.aws` directory
under a `profile`.  See `AWS configuration files <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html>`_ for directions on how to
configure your credentials and config files. In fact, initially you can assign a programmtic user keys to the `[default]` profile, so you don't have to
pass any credentials or region in to AWSimple.

For production, the `access key`, `secret access key`, and `region` can be provided to AWSimple directly, in a manner that is appropriate for your application.

Note that **AWS credentials must be properly managed and kept secret**, just as you would do for any other site where money is concerned.
There are little to no mechanisms in AWS to stop improper use of AWS resources. While billing alerts can and should be used, these are "after the fact" and
will not necessarily prevent billing surprises.

See the AWS documentation on `configuration files <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html>`_ for more information.

Testing your AWS Account
~~~~~~~~~~~~~~~~~~~~~~~~
Dealing with IAM and permissions can be tedious, and difficult to test. If they are wrong, you merely get a permissions error. To help permissions debug,
AWSimple has a test feature to make sure you have the basic IAM setup working:

.. code:: python

    from awsimple import AWSAccess

    # In this example we're using the default
    # IAM profile (in ~/.aws/credentials and ~/.aws/config)
    print(AWSAccess().test())  # Should be 'True'

If this prints `True`, you at least have properly configured your programmatic user for AWSimple to use.

Services accessible with AWSimple
---------------------------------
AWSimple offers access into :ref:`S3`, :ref:`DynamoDB`, :ref:`SNS`, and :ref:`SQS`.


S3
--
S3 is probably one of the most popular AWS services. S3 is based on `buckets` and `objects` within those buckets. Again, AWSimple assumes a basic
knowledge of S3, but refer to the `S3 documentation <https://aws.amazon.com/s3/>`_ if you are unfamiliar with S3.

AWSimple provides the ability to create and delete S3 buckets, and write and read S3 bucket objects. In addition a few helper methods exist
such as listing buckets and bucket objects.

S3 create bucket
~~~~~~~~~~~~~~~~~~
Before you can use a bucket, it needs to be created. A bucket can be created with the AWS console, but here we'll do it programmatically with AWSimple:

.. code:: python

    from awsimple import S3Access

    # bucket names are globally unique, so change this bucket name to something unique to you
    s3_access = S3Access("james-abel-awsimple-test-bucket")
    s3_access.create_bucket()

Each S3Access instance is associated with a specific bucket.

S3 write
~~~~~~~~
Now let's write an object to the bucket we just created:

.. code:: python

    # the S3 key is the name of the object in the S3 bucket, somewhat analogous to a file name
    s3_key = "hello.txt"

    # write our "hello world" message to S3
    s3_access.write_string("hello world", s3_key)

S3 read
~~~~~~~
And finally let's read the object back:

.. code:: python

    # will print "hello world"
    print(s3_access.read_string(s3_key))

S3 Caching
~~~~~~~~~~
AWSimple can use local caching to reduce network traffic, which in turn can reduce costs and speed up applications. A file hash (SHA512) is
used to ensure file content equivalency.

DynamoDB
--------
DynamoDB is a "NoSQL" (AKA document based) database. It is a "serverless" service that offers an `On Demand` option. DynamoDB is made up
of `tables`, and each table can store a collection `items`. These `items` are similar to JSON objects, and can
be created from Python dictionaries, with similar restrictions in converting Python dictionaries serializable to JSON. For these Python dicts,
DynamoDB allow you to store and retrieve those dicts to and from the cloud simply, quickly, and there is a
`free tier <https://aws.amazon.com/dynamodb/pricing/on-demand/>`_.

Probably the trickiest part is selecting the `primary key`. The `primary key` is what defines the uniqueness of an item.
See `AWS docs on primary key design <https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-design.html>`_ for details.
The basic idea is that the primary key must be unique to that item and is composed of either a single `partition` (or hash) key or a combination of
a `partition` and `sort` (or range) keys. Those keys are often either strings or numbers, although boolean is also allowed. Secondary
keys and indexes are also supported and can be used for queries.

The default type for partition and sort keys is a string (`str`), but numbers (using `int`) and booleans (using `bool`) can also be specified.

DynamoDB - Partition Key
~~~~~~~~~~~~~~~~~~~~~~~~
The code below shows how you can use the simple primary key (no sort key) in a table to put and get items. This also illustrates the
flexibility of a NoSQL database. Fields can be added after the table creation, as long as the primary key does not change.

.. code:: python

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
    item = dynamodb_access.get_item("email", "john@ledzeppelin.com")  # this is a "get" since we're using a key and will always get back exactly one item

DynamoDB - Partition and Sort Keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Below is an example of using a `composite` primary key, which is comprised of a `partition` key and a `sort` key.

.. code:: python

    dynamodb_access = DynamoDBAccess("musical_instruments_example", profile_name="testawsimple")

    # Our primary key is a composite of partition (manufacturer) and sort (serial_number).
    # For a particular manufacturer, serial numbers define exactly one instrument (for this example we are assuming a serial number can be represented as an
    # integer and doesn't have to be a string).
    dynamodb_access.create_table("manufacturer", "serial_number", sort_key_type=int)

    # we have to convert float to a Decimal for DynamoDB
    dynamodb_access.put_item(dict_to_dynamodb({"manufacturer": "Gibson", "serial_number": 1234, "model": "Ripper", "year": 1983, "price": 1299.50}))
    dynamodb_access.put_item(dict_to_dynamodb({"manufacturer": "Gibson", "serial_number": 5678, "model": "Thunderbird", "year": 1977, "price": 2399.50}))
    dynamodb_access.put_item(
        dict_to_dynamodb(
            {
                "manufacturer": "Fender",
                "serial_number": 1234,
                "model": "Precision",
                "year": 2008,
                "price": 1800.0,
            }  # same serial number as the Gibson Ripper, but that's OK since this is Fender
        )
    )

    # get all the Gibson instruments
    item = dynamodb_access.query("manufacturer", "Gibson")  # this can (and will in this case) be multiple items
    pprint(item)

DynamoDB Secondary Indexes
~~~~~~~~~~~~~~~~~~~~~~~~~~
You can add `secondary` indexes to a DynamoDB table in order to do a query on fields you didn't put in the original primary keys.
This is very similar to adding indexes in a relational database, which is often done to speed up queries.  Adding a secondary index
can also be done after table creation to facilitate new data, which is very handy when not all data is known at table
creation time.

DynamoDB Scanning and Caching
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Sometimes you want an entire table to do some sort of search or data-mining on. While AWS provides a `scan` capability which is available
in awsimple's `DynamoDBAccess.scan_table()` method, this reads the entire table for each scan. However, this can be slow and/or costly. In order
to reduce cost and increase speed, AWSimple offers a cached table scan via `DynamoDBAccess.scan_table_cached()` for tables that the
user *knows* are static or at least verify slowly changing. If course, it's up to the user of awsimple to determine which method to use - the
regular or cached version.

For convenience, AWSimple also looks at the table's item count to determine if the cached scan needs to invalidate the cache. This can be
useful if you know a table is only added to (thus the item count will change when it's updated) and you only try to access the table some time
after the update. As of this writing the table item count is updated roughly every 6 hours. For example, you may use DynamoDB to store
clinical trial data that is updated in a human time frame - e.g. weekly or even monthly, and once the trial is closed the data may never change.
These sorts of situations may be appropriate for cached table scans. Of course it's up to the programmer to ensure this caching is appropriate
for their use case. If not, use the regular `scan_table()` (albeit with the cost and performance implications).

SNS
---
SNS is AWS's Notification service for messages. SNS can create notifications for a variety of endpoints, including emails, text messages and
:ref:`SQS` queues. SNS can also be "connected" to other AWS services such as S3 so that S3 events (e.g. writes) can cause an S3 notification.

SQS
---
SQS is AWS's queuing service. Messages can be placed in queues (either programmatically or "connected" to other AWS services like SNS).
Programs can poll SQS queues to get messages to operate on. SQS queues can be immediately read (and return nothing of no messages are available)
or `long polled` to wait for an incoming message to act on.
