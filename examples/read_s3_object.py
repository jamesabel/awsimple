from ismain import is_main

from awsimple import S3Access


def read_s3_object():
    s3_access = S3Access("testawsimple")
    print(s3_access.read_string("helloworld.txt"))


if is_main():
    read_s3_object()
