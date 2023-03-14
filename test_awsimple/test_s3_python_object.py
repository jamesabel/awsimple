from awsimple import S3Access

from test_awsimple import test_awsimple_str


def test_s3_python_object():
    my_dict_a = {"a": 1}
    my_dict_b = {"b": 2}
    my_list = [1, 2, 3]
    my_complex_dict = {"1": 2, "my_list": [0, 9], "my_dict": {"z": -1, "w": -2}}

    s3_key = "my_object"
    s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)

    for my_object in (my_dict_a, my_dict_b, my_list, my_complex_dict):
        s3_access.upload_object_as_json(my_object, s3_key)

        my_dict_from_s3 = s3_access.download_object_as_json(s3_key)
        assert my_object == my_dict_from_s3

        my_dict_from_s3 = s3_access.download_object_as_json_cached(s3_key)
        assert my_object == my_dict_from_s3
        my_dict_from_s3 = s3_access.download_object_as_json_cached(s3_key)  # this will be the cached version
        assert my_object == my_dict_from_s3
        assert s3_access.download_status.cache_hit
