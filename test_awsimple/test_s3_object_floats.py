

def test_s3_object_floats(s3_access):
    object_with_floats = {"0.1": 2.3456789E-11}
    s3_key = "a"
    s3_access.upload_object_as_json(object_with_floats, s3_key)
    s3_object = s3_access.download_object_as_json_cached(s3_key)
    print(s3_object)
    assert s3_object == object_with_floats
