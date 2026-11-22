"""
A bucket listing that makes no request per object, and the options of S3Access.download() (issue 54, items 1 and 3).

All tests, helpers and constants in this file are AI-generated (Claude Code).
"""

from collections import Counter
from contextlib import contextmanager
from pathlib import Path
from threading import Lock

import pytest
from boto3.s3.transfer import TransferConfig
from s3transfer.manager import TransferManager

import awsimple.s3
from awsimple import AWSimpleException, BucketNotFound, S3Access, S3ListedObject
from test_awsimple import temp_dir, test_awsimple_str

list_prefix = "list_objects/plain/"  # nothing else writes under this prefix: a real bucket keeps the objects of earlier runs and of other tests


@contextmanager
def _count_requests(s3_access: S3Access):
    """
    Count the requests an S3Access makes to S3, by operation (e.g. {"HeadObject": 1, "GetObject": 1}), in the dict this yields.
    """
    requests: Counter = Counter()
    lock = Lock()  # the parts of a download are requested from threads at once

    def _count(model, **_unused_kwargs):
        with lock:
            requests[model.name] += 1

    for client in (s3_access.client, s3_access.resource.meta.client):
        client.meta.events.register("before-call.s3", _count)
    try:
        yield requests
    finally:
        for client in (s3_access.client, s3_access.resource.meta.client):
            client.meta.events.unregister("before-call.s3", _count)


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
    # no request for the metadata is made at all: not by awsimple, and not by the transfer manager (which gets the size and ETag from the metadata)
    s3_key = "download_options/with_metadata.txt"
    s3_access.write_string("the caller already has the metadata", s3_key)
    s3_object_metadata = s3_access.get_s3_object_metadata(s3_key)
    dest_path = Path(temp_dir, "download_options", "with_metadata.txt")
    dest_path.unlink(missing_ok=True)

    with monkeypatch.context() as patch:
        patch.setattr(s3_access, "get_s3_object_metadata", _no_head_object)
        with _count_requests(s3_access) as requests:
            assert s3_access.download(s3_key, dest_path, s3_object_metadata)

    assert requests == {"GetObject": 1}
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

        def _transfer_manager(client, config, **kwargs):
            used_configs.append(config)
            return TransferManager(client, config, **kwargs)

        monkeypatch.setattr(awsimple.s3, "TransferManager", _transfer_manager)
        dest_path = Path(temp_dir, "download_options", f"transfer_config_{s3_access_class.__name__}.txt")
        assert s3_access.download(s3_key, dest_path)
        assert dest_path.read_text() == "transfer config"

    assert [used_config.use_threads for used_config in used_configs] == [True, False]  # the default is threads, as downloads have always used


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.parametrize("size", [100, 20 * 1024 * 1024])  # one request for the contents, and (above the multipart threshold) several requests for parts of them
def test_s3_download_makes_one_metadata_request(tmp_path, size):
    # download() makes one request for the metadata of the S3 object. The transfer manager makes none of its own (it did, so a download was two metadata requests).
    s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str, cache_dir=Path(tmp_path, "cache"))
    s3_key = f"download_options/one_metadata_request_{size}.bin"
    source_path = Path(tmp_path, "source.bin")
    source_path.write_bytes(bytes(size))
    s3_access.upload(source_path, s3_key, force=True)
    dest_path = Path(tmp_path, "dest.bin")

    with _count_requests(s3_access) as requests:
        assert s3_access.download(s3_key, dest_path)
    assert requests["HeadObject"] == 1
    assert set(requests) == {"HeadObject", "GetObject"}
    assert requests["GetObject"] == (1 if size == 100 else 3)  # parts of 8 MiB
    assert dest_path.stat().st_size == size

    # with the cache: a miss is the same one metadata request and the download, and a hit is the one metadata request (which is how a changed object is seen)
    with _count_requests(s3_access) as requests:
        assert not s3_access.download_cached(s3_key, dest_path).cache_hit
    assert requests["HeadObject"] == 1 and set(requests) == {"HeadObject", "GetObject"}
    with _count_requests(s3_access) as requests:
        assert s3_access.download_cached(s3_key, dest_path).cache_hit
    assert requests == {"HeadObject": 1}


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_download_object_that_does_not_exist(s3_access, tmp_path):
    # found out by the one metadata request, so it is raised at once and not tried again
    with _count_requests(s3_access) as requests, pytest.raises(AWSimpleException):
        s3_access.download("download_options/does_not_exist.txt", Path(tmp_path, "does_not_exist.txt"))
    assert requests == {"HeadObject": 1}
    assert not Path(tmp_path, "does_not_exist.txt").exists()
