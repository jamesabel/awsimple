import re
from getpass import getuser


def get_example_bucket_name() -> str:
    """
    Get the name of the S3 bucket that the examples use. Bucket names are globally unique, so the name has your user name in it (change it to something unique to you
    for your own code). A bucket name can only have lower case letters, digits and hyphens, so the user name is made lower case and any other character is replaced.

    :return: S3 bucket name
    """
    return "awsimple-test-bucket-" + re.sub(r"[^a-z0-9-]", "-", getuser().lower())
