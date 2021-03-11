AWSimple - a simple AWS API
===========================

*(pronounced A-W-Simple)*

AWSimple provides a simple, object-oriented interface into four AWS "serverless" cloud services:

- S3 - Binary object storage. Analogous to storing files in the cloud.
- DynamoDB - A NoSQL database to put, get, and query dictionary-like objects.
- SQS - Queuing service for sending and receiving messages.
- SNS - Notification service to send messages to a variety of destinations including emails, SMS messages, and SQS queues.

`AWSimple` also provides some additional features:

- True file hashing (SHA512) for S3 files.
- Locally cached S3 accesses.
- DynamoDB full table scans (with local cache option).
- Built-in pagination.

If you're new to `AWSimple`, check out the :ref:`Quick Start Guide`. Also check out the
`examples <https://github.com/jamesabel/awsimple/tree/main/examples>`_.

.. toctree::
    :maxdepth: 2

    quick_start_guide
    user_guide
    aws_access
    s3_access
    dynamodb_access
    sns_access
    sqs_access
    thank_you


Testing
-------
.. include:: coverage.txt


Indices and tables
==================
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


The `AWSimple documentation <https://awsimple.readthedocs.io/>`_ is hosted on `Read the Docs <https://www.readthedocs.org/>`_ .
