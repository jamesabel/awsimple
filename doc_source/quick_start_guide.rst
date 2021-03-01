
Quick Start Guide
=================


Installation
------------

Install `awsimple` from PyPI:

    `pip install awsimple`

AWS's IAM
---------

First you need to determine how you're going to access AWS, which is through AWS's IAM (Identity and Access Management).  There are two ways:

- `Use keys directly`: your AWS Access Key and AWS Secret Access Key are passed directly into AWSimple.
- `Use an AWS profile`: An `.aws` directory in your home directory contains CONFIG and CREDENTIALS files that contain profiles that contain your Access Key and Secret Access Key.

For development, the profile method is recommended. This way your secrets are kept out of your repository and
application. In fact, if you put your secrets in a `default` profile, you don't have to tell AWSimple anything about your
credentials at all since they will be used from the default location and profile.

For applications, you usually don't want to use an `.aws` directory with profiles. Rather, you pass in keys in some
secure mechanism defined by your particular application.

Note that **AWS credentials must be properly managed and kept secret**, just as you would do for any other site where money is concerned.
There are little to no mechanisms in AWS to stop improper use of AWS resources. While billing alerts can and should be used, these are "after the fact" and
will not necessarily prevent billing surprises.

See the AWS documentation on `configuration files <https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html>`_ for more information.

Testing the AWS Connection
--------------------------

Now that you have your AWS IAM configured, let's test it out:

.. code:: python

    from awsimple import AWSAccess

    # In this example we're using the default profile
    print(AWSAccess().test())  # Should be 'True'


If everything worked OK, this code will output `True` and you can go on to the next section.

Creating, Writing and Reading an S3 Bucket Object
-------------------------------------------------

Assuming your IAM configuration allows you to create an AWS S3 bucket and object, let's to that now.

.. code:: python

    from awsimple import S3Access


    # the S3 key is the name of the object in the S3 bucket, somewhat analogous to a file name
    s3_key = "hello.txt"


    # bucket names are globally unique, so change this bucket name to something unique to you
    s3_access = S3Access("james-abel-awsimple-test-bucket")


    # let's first make sure the bucket exists
    s3_access.create_bucket()

    # write our message to S3
    s3_access.write_string("hello world", s3_key)


    # will output "hello world"
    print(s3_access.read_string(s3_key))
