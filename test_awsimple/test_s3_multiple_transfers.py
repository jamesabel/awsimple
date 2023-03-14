from pathlib import Path
import pytest
from shutil import rmtree

from awsimple import AWSimpleException, is_mock

from test_awsimple import temp_dir, cache_dir


def check_file_contents(file_path: Path, expected_contents: str):
    with file_path.open() as f:
        file_contents = f.read()
        assert file_contents == expected_contents


def test_s3_multiple_transfers(s3_access):
    s3_paths = {}
    rmtree(temp_dir)
    for test_string in ["a", "b"]:
        s3_paths[test_string] = {}
        for mode in ["in", "out"]:
            p = Path(temp_dir, mode, f"{test_string}.txt")
            p.parent.mkdir(parents=True, exist_ok=True)
            if mode == "in":
                with p.open("w") as f:
                    f.write(test_string)
            s3_paths[test_string][mode] = p

    if is_mock():
        with pytest.raises(AWSimpleException):
            s3_access.download_cached("a", s3_paths["a"]["out"])  # won't exist at first if mocked

    # upload and download file
    s3_access.upload(s3_paths["a"]["in"], "a")
    download_status = s3_access.download_cached("a", s3_paths["a"]["out"])
    assert download_status.success
    assert not download_status.cache_hit
    assert download_status.cache_write
    check_file_contents(s3_paths["a"]["out"], "a")

    # upload a different file into same bucket and check that we get the contents of that new file
    s3_access.upload(s3_paths["b"]["in"], "a")
    download_status = s3_access.download_cached("a", s3_paths["a"]["out"])
    assert download_status.success
    assert not download_status.cache_hit
    assert download_status.cache_write
    check_file_contents(s3_paths["a"]["out"], "b")

    # cached download
    download_status = s3_access.download_cached("a", s3_paths["a"]["out"])
    assert download_status.success
    assert download_status.cache_hit
    assert not download_status.cache_write
    check_file_contents(s3_paths["a"]["out"], "b")

    # put "a" back and just use regular download (not cached)
    s3_access.upload(s3_paths["a"]["in"], "a")
    assert s3_access.download("a", s3_paths["a"]["out"])
    check_file_contents(s3_paths["a"]["out"], "a")

    # write something else to that bucket
    s3_access.write_string("c", "a")
    assert s3_access.read_string("a") == "c"

    # now upload and download an object
    test_dict = {"z": 3}
    s3_access.upload_object_as_json(test_dict, "a")
    downloaded_dict = s3_access.download_object_as_json("a")
    assert test_dict == downloaded_dict
    downloaded_dict = s3_access.download_object_as_json_cached("a")
    assert test_dict == downloaded_dict

    assert len(list(cache_dir.glob("*"))) == 3  # there should be 3 entries in the cache at this point
