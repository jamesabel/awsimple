import pytest


def test_s3_delete(s3_access):
    test_string = "hi"
    s3_key = "hi.txt"
    s3_access.write_string(test_string, s3_key)  # will create if the bucket doesn't exist
    assert s3_access.read_string(s3_key) == test_string
    s3_access.delete_object(s3_key)
    with pytest.raises(s3_access.client.exceptions.NoSuchKey):
        s3_access.read_string(s3_key)
