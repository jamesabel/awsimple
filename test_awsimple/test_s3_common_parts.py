"""
The common parts that the S3 code is made of, each on its own: the retry of a transfer, the plan of a sync (which does not access S3), the content cache and the mark of
a cache file as used.

All tests, fixtures, helpers and constants in this file are AI-generated (Claude Code).
"""

import hashlib
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
from botocore.exceptions import ClientError

import awsimple
from awsimple import AWSimpleException, S3Access, S3ContentCache, S3ListedObject, S3ObjectMetadata, S3SyncOutcome, lru_cache_mark_used
from awsimple.s3_sync import plan_sync
from test_awsimple import test_awsimple_str

s3_mtime = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


def _metadata(content: bytes, with_sha512: bool) -> S3ObjectMetadata:
    metadata_sha512 = hashlib.sha512(content).hexdigest() if with_sha512 else None
    return S3ObjectMetadata("bucket", "key", len(content), s3_mtime, "etag", None, "url", metadata_sha512)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_retry(s3_access, monkeypatch):
    monkeypatch.setattr(s3_access, "retry_sleep_time", 0.0)
    monkeypatch.setattr(s3_access, "retry_count", 4)
    client_error = ClientError({"Error": {"Code": "SlowDown", "Message": "forced by the test"}}, "GetObject")

    # an attempt that is not successful, an attempt that raises an error that is tried again, then a successful attempt
    attempts = []

    def _third_time(attempt_number: int) -> bool:
        attempts.append(attempt_number)
        if attempt_number == 1:
            raise client_error
        return attempt_number == 2

    s3_access._retry(_third_time, "do what the test does")
    assert attempts == [0, 1, 2]

    # never successful
    attempts.clear()
    with pytest.raises(AWSimpleException, match="couldn't do what the test does after 4 attempts"):
        s3_access._retry(lambda attempt_number: attempts.append(attempt_number) is not None, "do what the test does")
    assert attempts == [0, 1, 2, 3]

    # an error that is not one to try again is raised as it is
    def _value_error(_unused_attempt_number: int) -> bool:
        raise ValueError("not a connection error")

    with pytest.raises(ValueError):
        s3_access._retry(_value_error, "do what the test does")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_plan_sync_without_s3(tmp_path):
    # the plan of a sync is made from a listing and the local files alone
    def _listed(s3_key: str, size: int, storage_class: str = "STANDARD") -> S3ListedObject:
        return S3ListedObject(s3_key, size, s3_mtime, "etag", storage_class)

    listed_objects = {
        listed_object.key: listed_object
        for listed_object in (_listed("p/current.txt", 7), _listed("p/changed.txt", 7), _listed("p/new.txt", 3), _listed("p/folder/", 0), _listed("p/archived.txt", 5, "DEEP_ARCHIVE"))
    }
    for name, content in (("current.txt", "current"), ("changed.txt", "longer than 7")):
        Path(tmp_path, name).write_text(content)
        os.utime(Path(tmp_path, name), (s3_mtime.timestamp(), s3_mtime.timestamp()))

    plan = plan_sync(listed_objects, tmp_path, "p/", False, 10.0)
    assert list(plan) == list(listed_objects)  # the order of the listing
    assert {s3_key: sync_object.outcome for s3_key, sync_object in plan.items()} == {
        "p/current.txt": S3SyncOutcome.up_to_date,
        "p/changed.txt": S3SyncOutcome.would_download,
        "p/new.txt": S3SyncOutcome.would_download,
        "p/folder/": S3SyncOutcome.skipped,
        "p/archived.txt": S3SyncOutcome.skipped,
    }
    assert plan["p/new.txt"].local_path == Path(tmp_path, "new.txt")
    assert sorted(path.name for path in tmp_path.iterdir()) == ["changed.txt", "current.txt"]  # a plan changes nothing


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.parametrize("with_sha512", [True, False])
def test_s3_content_cache(tmp_path, with_sha512):
    # the cache on its own: with the SHA-512 of the S3 object, and (for an S3 object that has none) through the index
    content = b"cached contents"
    s3_object_metadata = _metadata(content, with_sha512)
    content_cache = S3ContentCache(Path(tmp_path, "cache"), 1000, None)
    dest_path = Path(tmp_path, "dest", "copy.txt")

    assert content_cache.find(s3_object_metadata) is None
    assert content_cache.read(s3_object_metadata) is None
    assert not content_cache.copy_to(s3_object_metadata, dest_path)
    assert not dest_path.exists()

    assert content_cache.write(s3_object_metadata, content, hashlib.sha512(content).hexdigest())
    assert content_cache.find(s3_object_metadata) == Path(tmp_path, "cache", hashlib.sha512(content).hexdigest())
    assert content_cache.read(s3_object_metadata) == content
    assert content_cache.copy_to(s3_object_metadata, dest_path)
    assert dest_path.read_bytes() == content
    assert dest_path.stat().st_mtime == pytest.approx(s3_mtime.timestamp(), abs=2.0)
    assert Path(tmp_path, "cache", "index").exists() == (not with_sha512)

    # contents that are larger than the cache are not cached, which is not an error
    large_content = b"x" * 2000
    assert not content_cache.write(_metadata(large_content, with_sha512), large_content, hashlib.sha512(large_content).hexdigest())

    # contents that do not have the SHA-512 of the S3 object are not cached
    if with_sha512:
        with pytest.raises(AWSimpleException):
            content_cache.write(s3_object_metadata, b"other contents", hashlib.sha512(b"other contents").hexdigest())


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_lru_cache_mark_used(tmp_path):
    cache_file_path = Path(tmp_path, "entry")
    cache_file_path.write_bytes(b"entry")
    long_ago = time.time() - 1000000.0
    os.utime(cache_file_path, (long_ago, long_ago))
    lru_cache_mark_used(cache_file_path)
    assert cache_file_path.stat().st_atime == pytest.approx(time.time(), abs=60.0)
    assert cache_file_path.stat().st_mtime == pytest.approx(long_ago, abs=2.0)
    lru_cache_mark_used(Path(tmp_path, "evicted"))  # a file that is no longer there is not an error


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_names_are_importable_from_where_they_were():
    # the S3 code is in several modules: its names are still in awsimple.s3, and BucketNotFound is the same exception everywhere
    from awsimple import exceptions, s3, s3_cache, s3_object, s3_paths, s3_sync

    assert s3.S3ObjectMetadata is s3_object.S3ObjectMetadata is awsimple.S3ObjectMetadata
    assert s3.s3_key_to_local_path is s3_paths.s3_key_to_local_path is awsimple.s3_key_to_local_path
    assert s3.S3SyncResult is s3_sync.S3SyncResult is awsimple.S3SyncResult
    assert s3.cache_index_dir_name == s3_cache.cache_index_dir_name
    assert s3.BucketNotFound is exceptions.BucketNotFound is awsimple.BucketNotFound
    assert s3.sha512_string == s3_object.sha512_string
    assert isinstance(S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)._get_content_cache(), S3ContentCache)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_lru_cache_write_nothing_left_to_evict(tmp_path, monkeypatch):
    # The cache was measured as full, but by the time room is made there is nothing in it to evict (another thread or process evicted it all, which is what the
    # measured size stands in for here). The write does not go on for ever: it gives up, which is a normal outcome of a cache write.
    from awsimple import cache

    cache_dir = Path(tmp_path, "cache")
    cache_dir.mkdir()
    monkeypatch.setattr(cache, "get_directory_size", lambda _unused_path: 1000)
    assert not cache.lru_cache_write(b"x" * 10, cache_dir, "new", max_absolute_cache_size=100)
    assert list(cache_dir.iterdir()) == []


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_bucket_exists_with_aws_uses_short_timeouts(s3_access, monkeypatch):
    # With AWS (not the moto mock), bucket_exists() makes a client of its own with short timeouts, so that a bucket that does not exist is found out quickly.
    # No AWS call is made here: the client of its own is replaced with the mock's.
    made_with = []

    def _client(service_name, config):
        made_with.append((service_name, config))
        return s3_access.client

    monkeypatch.setattr(s3_access, "is_mocked", lambda: False)
    monkeypatch.setattr(s3_access.session, "client", _client)
    assert s3_access.bucket_exists()
    ((service_name, config),) = made_with
    assert service_name == "s3"
    assert config.connect_timeout == 5
    assert config.retries["max_attempts"] == 3
