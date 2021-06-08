from pathlib import Path
import time

from awsimple import S3Access
from requests import get

from test_awsimple import test_awsimple_str


def test_s3_upload():
    contents = "I am public readable"
    s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)
    s3_access.set_public_readable(True)
    test_file_name = "public_readable.txt"
    test_file_path = Path(Path("temp"), test_file_name)
    test_file_path.open("w").write(contents)
    assert s3_access.upload(test_file_path, test_file_name, force=True)
    time.sleep(3)
    assert s3_access.object_exists(test_file_name)

    # read from the URL to see if the contents are public readable
    metadata = s3_access.get_s3_object_metadata(test_file_name)
    object_contents = get(metadata.url).content.decode("utf-8")
    assert object_contents == contents
