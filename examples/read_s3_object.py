from ismain import is_main

from awsimple import S3Access

from bucket_name import get_example_bucket_name


def read_s3_object():
    # reads the S3 object that write_read_s3_object.py wrote, so run that first (this uses your default AWS profile)
    s3_access = S3Access(get_example_bucket_name())
    print(s3_access.read_string("hello.txt"))  # hello.txt is the S3 object key


if is_main():
    read_s3_object()
