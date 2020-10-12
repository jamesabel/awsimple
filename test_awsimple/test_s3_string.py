from awsimple import S3Access

from test_awsimple import test_awsimple_str


def test_s3_string():
    s3_access = S3Access(test_awsimple_str)
    s3_access.write_string(test_awsimple_str, test_awsimple_str)
    d = s3_access.dir()
    metadata = d[test_awsimple_str]
    assert metadata.size == len(test_awsimple_str)
    assert metadata.key == test_awsimple_str  # the contents are the same as the key
    # https://passwordsgenerator.net/sha512-hash-generator/
    assert metadata.sha512.lower() == 'D16764F12E4D13555A88372CFE702EF8AE07F24A3FFCEDE6E1CDC8B7BFC2B18EC3468A7752A09F100C9F24EA2BC77566A08972019FC04CF75AB3A64B475BDFA3'.lower()
