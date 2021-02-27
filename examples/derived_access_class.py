from ismain import is_main

from awsimple import S3Access


class ExampleStorageAccess(S3Access):
    def __init__(self, bucket: str, **kwargs):
        super().__init__(bucket, profile_name="testawsimple", **kwargs)


def read_s3_object():
    # profile_name provided by ExampleCloudStorageAccess
    s3_access = ExampleStorageAccess("testawsimple")  # bucket name
    print(s3_access.read_string("helloworld.txt"))


if is_main():
    read_s3_object()
