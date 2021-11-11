
from awsimple import S3Access

from test_awsimple import test_awsimple_str


def test_s3_python_object():
    my_dict = {"a": 1}
    s3_key = "my_dict.json"
    s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)
    s3_access.upload_object_as_json(my_dict, s3_key)
    my_dict_from_s3 = s3_access.download_object_as_json(s3_key)
    assert my_dict == my_dict_from_s3
