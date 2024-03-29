from pprint import pprint
from pathlib import Path

from awsimple import S3Access

from test_awsimple import test_awsimple_str, temp_dir


def test_s3_keys():
    s3_access = S3Access(test_awsimple_str, profile_name=test_awsimple_str)  # use non-keyword parameter for bucket_name

    # set up
    s3_access.create_bucket()  # may already exist
    test_file_name = "test.txt"
    test_file_name_2 = "test2.txt"
    test_file_path = Path(temp_dir, test_file_name)
    test_file_path.open("w").write("hello world")
    s3_access.upload(test_file_path, test_file_name_2)  # may already be in S3
    s3_access.upload(test_file_path, test_file_name)  # may already be in S3

    s3_keys = s3_access.keys()
    pprint(s3_keys)
    # for real AWS I may have other objects in the test bucket
    assert test_file_name in s3_keys
    assert test_file_name_2 in s3_keys


def test_s3_keys_prefix():
    s3_access = S3Access(test_awsimple_str, profile_name=test_awsimple_str)  # use non-keyword parameter for bucket_name

    # set up
    s3_access.create_bucket()  # may already exist
    test_file_name = "test.txt"
    test_file_name_2 = "test2.txt"
    test_file_path = Path(temp_dir, test_file_name)
    test_file_path.open("w").write("hello world")
    s3_access.upload(test_file_path, test_file_name_2)  # may already be in S3
    s3_access.upload(test_file_path, test_file_name)  # may already be in S3

    s3_keys = s3_access.keys("test2")
    pprint(s3_keys)
    # for real AWS I may have other objects in the test bucket
    assert test_file_name not in s3_keys
    assert test_file_name_2 in s3_keys
