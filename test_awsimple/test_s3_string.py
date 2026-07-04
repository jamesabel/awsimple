from awsimple import S3Access, get_bytes_crc64nvme

from test_awsimple import test_awsimple_str


def test_s3_string():
    s3_access = S3Access(test_awsimple_str)
    s3_access.write_string(test_awsimple_str, test_awsimple_str)
    d = s3_access.dir()
    metadata = d[test_awsimple_str]
    assert metadata.size == len(test_awsimple_str)
    assert metadata.key == test_awsimple_str  # the contents are the same as the key
    assert metadata.crc64nvme == get_bytes_crc64nvme(test_awsimple_str.encode())
