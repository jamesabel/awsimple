"""
A bucket listing that makes no request per object, and the options of S3Access.download() (issue 54, items 1 and 3).

All tests, helpers and constants in this file are AI-generated (Claude Code).
"""

from pathlib import Path

import pytest
from boto3.s3.transfer import TransferConfig

from awsimple import BucketNotFound, S3Access, S3ListedObject
from test_awsimple import temp_dir, test_awsimple_str

list_prefix = "list_objects/plain/"  # nothing else writes under this prefix: a real bucket keeps the objects of earlier runs and of other tests


def _no_head_object(*_unused_args, **_unused_kwargs):
    raise AssertionError("a request for the metadata of an object was made")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_list_objects(s3_access, monkeypatch):
    contents = {f"{list_prefix}a.txt": "a", f"{list_prefix}sub/b.txt": "bb", f"{list_prefix}c.txt": "ccc"}
    for s3_key, content in contents.items():
        s3_access.write_string(content, s3_key)
    metadata = {s3_key: s3_access.get_s3_object_metadata(s3_key) for s3_key in contents}

    with monkeypatch.context() as patch:
        patch.setattr(s3_access.client, "head_object", _no_head_object)  # the listing makes no request per object
        listed_objects = s3_access.list_objects(list_prefix)

    assert list(listed_objects) == sorted(contents)  # only the prefix, in the order of the bucket listing
    for s3_key, listed_object in listed_objects.items():
        assert isinstance(listed_object, S3ListedObject)
        assert listed_object.key == s3_key
        assert listed_object.size == len(contents[s3_key]) == metadata[s3_key].size
        assert listed_object.mtime == metadata[s3_key].mtime
        assert listed_object.etag == metadata[s3_key].etag
        assert listed_object.storage_class == "STANDARD"

    assert set(listed_objects) <= set(s3_access.list_objects())  # no prefix lists the whole bucket
    assert s3_access.list_objects("list_objects_no_such_prefix/") == {}


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_list_objects_storage_class(s3_access):
    s3_key = "list_objects/storage_class/glacier.txt"
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=b"archived", StorageClass="GLACIER")
    assert s3_access.list_objects("list_objects/storage_class/")[s3_key].storage_class == "GLACIER"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_list_objects_bucket_not_found():
    with pytest.raises(BucketNotFound):
        S3Access(profile_name=test_awsimple_str, bucket_name="doesnotexist").list_objects()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_download_with_metadata_gets_no_metadata(s3_access, monkeypatch):
    # awsimple does not get the metadata again (boto3 still makes its own request for the size of the object)
    s3_key = "download_options/with_metadata.txt"
    s3_access.write_string("the caller already has the metadata", s3_key)
    s3_object_metadata = s3_access.get_s3_object_metadata(s3_key)
    dest_path = Path(temp_dir, "download_options", "with_metadata.txt")
    dest_path.unlink(missing_ok=True)

    with monkeypatch.context() as patch:
        patch.setattr(s3_access, "get_s3_object_metadata", _no_head_object)
        assert s3_access.download(s3_key, dest_path, s3_object_metadata)

    assert dest_path.read_text() == "the caller already has the metadata"
    assert dest_path.stat().st_mtime == pytest.approx(s3_object_metadata.mtime.timestamp(), abs=s3_access.mtime_abs_tol)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_download_with_out_of_date_metadata_gets_new_metadata(s3_access):
    # the metadata the caller has is from before the object was written again: the check against it fails, and the download is tried again with new metadata
    s3_key = "download_options/out_of_date_metadata.txt"
    s3_access.write_string("first", s3_key)
    out_of_date_metadata = s3_access.get_s3_object_metadata(s3_key)
    s3_access.write_string("written again and longer", s3_key)

    retrying_s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str, cache_dir=s3_access.cache_dir)
    retrying_s3_access.retry_sleep_time = 0.0
    dest_path = Path(temp_dir, "download_options", "out_of_date_metadata.txt")
    assert retrying_s3_access.download(s3_key, dest_path, out_of_date_metadata)
    assert dest_path.read_text() == "written again and longer"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_download_callback(s3_access):
    s3_key = "download_options/callback.txt"
    content = "progress is reported to the caller" * 100
    s3_access.write_string(content, s3_key)
    transferred = []
    assert s3_access.download(s3_key, Path(temp_dir, "download_options", "callback.txt"), callback=transferred.append)
    assert sum(transferred) == len(content)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_download_transfer_config(monkeypatch):
    class SingleThreadDownload(S3Access):
        def get_s3_download_transfer_config(self) -> TransferConfig:
            return TransferConfig(use_threads=False)

    s3_key = "download_options/transfer_config.txt"
    used_configs = []
    for s3_access_class in (S3Access, SingleThreadDownload):
        s3_access = s3_access_class(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)
        s3_access.write_string("transfer config", s3_key)
        download_file = s3_access.client.download_file

        def _download_file(*args, _download_file=download_file, **kwargs):
            used_configs.append(kwargs["Config"])
            return _download_file(*args, **kwargs)

        monkeypatch.setattr(s3_access.client, "download_file", _download_file)
        dest_path = Path(temp_dir, "download_options", f"transfer_config_{s3_access_class.__name__}.txt")
        assert s3_access.download(s3_key, dest_path)
        assert dest_path.read_text() == "transfer config"

    assert [used_config.use_threads for used_config in used_configs] == [True, False]  # the default is threads, as downloads have always used
