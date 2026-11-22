from ismain import is_main

from awsimple import S3Access

from bucket_name import get_example_bucket_name

profile_name = "testawsimple"  # all of my derived classes use this AWS profile name


class MyS3Access(S3Access):
    """
    MyS3Access class takes care of IAM via a profile name
    """

    def __init__(self, bucket: str, **kwargs):
        # define the profile name, but pass all other optional arguments to the base class
        super().__init__(bucket, profile_name=profile_name, **kwargs)


def write_read_s3_object():
    s3_access = MyS3Access(get_example_bucket_name())  # profile_name provided by MyS3Access
    s3_access.create_bucket()  # make sure the bucket exists
    s3_access.write_string("hello world", "hello.txt")  # hello.txt is the S3 object key
    print(s3_access.read_string("hello.txt"))


if is_main():
    write_read_s3_object()
