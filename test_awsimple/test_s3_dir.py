from pprint import pprint
from pathlib import Path

from awsimple import S3Access, get_bytes_crc64nvme

from test_awsimple import test_awsimple_str, temp_dir


def test_s3_dir():
    s3_access = S3Access(test_awsimple_str, profile_name=test_awsimple_str)  # use non-keyword parameter for bucket_name

    # set up
    s3_access.create_bucket()  # may already exist
    test_file_name = "test.txt"
    test_file_path = Path(temp_dir, test_file_name)
    test_file_path.open("w").write("hello world")
    s3_access.upload(test_file_path, test_file_name)  # may already be in S3

    s3_dir = s3_access.dir()
    pprint(s3_dir)
    md = s3_dir[test_file_name]
    assert md.key == test_file_name
    assert md.crc64nvme == get_bytes_crc64nvme(b"hello world")


def test_s3_dir_prefix():
    s3_access = S3Access(test_awsimple_str, profile_name=test_awsimple_str)  # use non-keyword parameter for bucket_name

    # set up
    s3_access.create_bucket()  # may already exist
    test_file_name = "test.txt"
    test_file_path = Path(temp_dir, test_file_name)
    test_file_path.open("w").write("hello world")
    s3_access.upload(test_file_path, test_file_name)  # may already be in S3

    s3_dir = s3_access.dir("test")
    pprint(s3_dir)
    md = s3_dir[test_file_name]
    assert md.key == test_file_name
    assert md.crc64nvme == get_bytes_crc64nvme(b"hello world")
