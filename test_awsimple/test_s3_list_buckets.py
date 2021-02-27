from pprint import pprint

from awsimple import S3Access

from test_awsimple import test_awsimple_str


def test_s3_list_buckets():
    bucket_names = S3Access().bucket_list()
    pprint(bucket_names)
    assert test_awsimple_str in bucket_names
