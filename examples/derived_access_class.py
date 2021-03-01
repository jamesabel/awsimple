from ismain import is_main

from awsimple import S3Access

profile_name = "testawsimple"  # all of my derived classes use this AWS profile name


class MyS3Access(S3Access):
    """
    MyS3Access class takes care of IAM via a profile name
    """

    def __init__(self, bucket: str, **kwargs):
        # define the profile name, but pass all other optional arguments to the base class
        super().__init__(bucket, profile_name=profile_name, **kwargs)


def read_s3_object():
    # profile_name provided by MyStorageAccess
    s3_access = MyS3Access("james-abel-awsimple-test-bucket")  # bucket name (for this example we assume it already exists)
    print(s3_access.read_string("hello.txt"))  # hello.txt is the S3 object key


if is_main():
    read_s3_object()
