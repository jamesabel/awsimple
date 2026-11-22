"""
What it takes for the S3 cache to give a cache hit: a cache file that has the contents it is named for (the SHA-512 of the S3 object), and that is still there.
Also that a cache hit counts as a use for the eviction of the least recently used file.

All tests, fixtures, helpers and constants in this file are AI-generated (Claude Code).
"""

import hashlib
import os
import time
from pathlib import Path

import pytest

from awsimple import S3Access, S3ContentCache, lru_cache_write
from awsimple.s3 import cache_index_dir_name
from test_awsimple import test_awsimple_str


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.fixture
def cache_s3_access(tmp_path) -> S3Access:
    # a cache of its own, so that only what a test caches is in it
    s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str, cache_dir=Path(tmp_path, "cache"))
    s3_access.create_bucket()
    return s3_access


def _cache_path(s3_access: S3Access, content: bytes) -> Path:
    return Path(s3_access.cache_dir, hashlib.sha512(content).hexdigest())


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_same_contents_are_one_cache_file(cache_s3_access, tmp_path):
    # the SHA-512 of an S3 object is the SHA-512 of its contents: two S3 objects written by awsimple with the same contents are one cache file, with no index
    content = b"the same contents"
    for name in ("one", "two"):
        cache_s3_access.write_string(content.decode(), f"cache_hits/same_{name}.txt")
    status = cache_s3_access.download_cached("cache_hits/same_one.txt", Path(tmp_path, "one.txt"))
    assert not status.cache_hit and status.cache_write
    assert cache_s3_access.download_cached("cache_hits/same_two.txt", Path(tmp_path, "two.txt")).cache_hit
    assert Path(tmp_path, "two.txt").read_bytes() == content
    assert [path.name for path in cache_s3_access.cache_dir.iterdir()] == [hashlib.sha512(content).hexdigest()]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_native_sha512_is_a_cache_hit(cache_s3_access, tmp_path):
    # S3 validated a native SHA-512, so contents with that SHA-512 that are in the cache are this S3 object's contents
    content = b"validated by S3"
    cache_s3_access.write_string(content.decode(), "cache_hits/native_source.txt")
    assert cache_s3_access.download_cached("cache_hits/native_source.txt", Path(tmp_path, "source.txt")).cache_write

    s3_key = "cache_hits/native_sha512.txt"
    cache_s3_access.client.put_object(Bucket=cache_s3_access.bucket_name, Key=s3_key, Body=content, ChecksumAlgorithm="SHA512")
    dest_path = Path(tmp_path, "native.txt")
    assert cache_s3_access.download_cached(s3_key, dest_path).cache_hit
    assert dest_path.read_bytes() == content


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cache_file_with_other_contents_is_not_used(cache_s3_access, tmp_path):
    # a cache file that does not have the contents it is named for (here: the same length) is not a cache hit, and is written again
    content = b"what is in S3"
    for s3_key, download in (("cache_hits/corrupt.txt", "file"), ("cache_hits/corrupt.json", "json")):
        cache_path = _cache_path(cache_s3_access, content if download == "file" else b'{"what": "is in S3"}')
        if download == "file":
            cache_s3_access.write_string(content.decode(), s3_key)
            dest_path = Path(tmp_path, "corrupt.txt")

            def _download(s3_key=s3_key, dest_path=dest_path):
                cache_s3_access.download_cached(s3_key, dest_path)
                return dest_path.read_bytes()
        else:
            cache_s3_access.upload_object_as_json({"what": "is in S3"}, s3_key)

            def _download(s3_key=s3_key):
                return cache_s3_access.download_object_as_json_cached(s3_key)

        good = _download()
        assert cache_s3_access.download_status.cache_write
        intact_cache_contents = cache_path.read_bytes()
        assert _download() == good
        assert cache_s3_access.download_status.cache_hit

        cache_path.write_bytes(intact_cache_contents.upper())
        assert cache_path.read_bytes() != intact_cache_contents
        assert _download() == good
        assert not cache_s3_access.download_status.cache_hit
        assert cache_s3_access.download_status.cache_write
        assert cache_path.read_bytes() == intact_cache_contents
        assert _download() == good
        assert cache_s3_access.download_status.cache_hit


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cache_hit_is_a_use(cache_s3_access, tmp_path):
    # the least recently used file is evicted by access time, which the OS does not reliably update on a read: a cache hit sets it
    content = b"used again"
    s3_key = "cache_hits/used.txt"
    cache_s3_access.client.put_object(Bucket=cache_s3_access.bucket_name, Key=s3_key, Body=content, ChecksumAlgorithm="CRC64NVME")  # no SHA-512, so it is found by the index
    dest_path = Path(tmp_path, "used.txt")
    cache_s3_access.download_cached(s3_key, dest_path)
    cache_path = _cache_path(cache_s3_access, content)
    (index_path,) = Path(cache_s3_access.cache_dir, cache_index_dir_name).iterdir()
    assert cache_path.stat().st_atime == pytest.approx(time.time(), abs=60.0)  # not the access or modification time of what it was copied from

    long_ago = time.time() - 30 * 24 * 60 * 60
    for path in (cache_path, index_path):
        os.utime(path, (long_ago, long_ago))
    assert cache_s3_access.download_cached(s3_key, dest_path).cache_hit
    for path in (cache_path, index_path):
        assert path.stat().st_atime == pytest.approx(time.time(), abs=60.0)
        assert path.stat().st_mtime == pytest.approx(long_ago, abs=2.0)  # only the access time


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cache_file_evicted_during_a_cache_hit(cache_s3_access, tmp_path, monkeypatch):
    # another process evicts the cache file after it was found and before it is read: the S3 object is downloaded instead
    content = b"evicted by another process"
    s3_key = "cache_hits/evicted.txt"
    cache_s3_access.write_string(content.decode(), s3_key)
    dest_path = Path(tmp_path, "evicted.txt")
    cache_s3_access.download_cached(s3_key, dest_path)
    cache_path = _cache_path(cache_s3_access, content)
    find = S3ContentCache.find

    def _evicted_after_found(content_cache, s3_object_metadata):
        found_path = find(content_cache, s3_object_metadata)
        assert found_path == cache_path
        cache_path.unlink()
        return found_path

    monkeypatch.setattr(S3ContentCache, "find", _evicted_after_found)
    dest_path.unlink()
    status = cache_s3_access.download_cached(s3_key, dest_path)
    assert status.success and not status.cache_hit and status.cache_write
    assert dest_path.read_bytes() == content
    assert cache_path.read_bytes() == content

    json_key = "cache_hits/evicted.json"
    cache_s3_access.upload_object_as_json({"evicted": True}, json_key)
    monkeypatch.undo()
    assert cache_s3_access.download_object_as_json_cached(json_key) == {"evicted": True}
    cache_path = _cache_path(cache_s3_access, b'{"evicted": true}')
    monkeypatch.setattr(S3ContentCache, "find", _evicted_after_found)
    assert cache_s3_access.download_object_as_json_cached(json_key) == {"evicted": True}
    assert not cache_s3_access.download_status.cache_hit
    assert cache_s3_access.download_status.cache_write


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_lru_cache_write_evicts_past_an_empty_file(tmp_path):
    # an empty file frees no space when it is evicted, which is not the same as nothing being left to evict
    cache_dir = Path(tmp_path, "cache")
    cache_dir.mkdir()
    now = time.time()
    for name, content, access_time in (("empty", b"", now - 1000.0), ("old", b"x" * 80, now - 500.0)):
        Path(cache_dir, name).write_bytes(content)
        os.utime(Path(cache_dir, name), (access_time, access_time))
    assert lru_cache_write(b"y" * 50, cache_dir, "new", max_absolute_cache_size=100)
    assert sorted(path.name for path in cache_dir.iterdir()) == ["new"]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cache_file_with_another_length_is_not_used(cache_s3_access, tmp_path):
    # e.g. a cache file that an older version did not finish writing
    content = b"all of the contents"
    s3_key = "cache_hits/truncated.txt"
    cache_s3_access.write_string(content.decode(), s3_key)
    dest_path = Path(tmp_path, "truncated.txt")
    cache_s3_access.download_cached(s3_key, dest_path)
    cache_path = _cache_path(cache_s3_access, content)
    cache_path.write_bytes(content[:5])
    status = cache_s3_access.download_cached(s3_key, dest_path)
    assert not status.cache_hit and status.cache_write
    assert dest_path.read_bytes() == content
    assert cache_path.read_bytes() == content


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_empty_object_is_cached(cache_s3_access, tmp_path):
    s3_key = "cache_hits/empty.txt"
    cache_s3_access.write_string("", s3_key)
    dest_path = Path(tmp_path, "empty.txt")
    status = cache_s3_access.download_cached(s3_key, dest_path)
    assert not status.cache_hit and status.cache_write
    assert cache_s3_access.download_cached(s3_key, dest_path).cache_hit
    assert dest_path.read_bytes() == b""
    assert _cache_path(cache_s3_access, b"").stat().st_size == 0


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_object_without_sha512_is_found_by_the_index(cache_s3_access, tmp_path):
    # only an S3 object that has no SHA-512 (not written by awsimple) has an index file: it holds the SHA-512 awsimple calculated, and the read does not change the S3 object
    content = b"written by another tool"
    s3_key = "cache_hits/no_sha512.txt"
    cache_s3_access.client.put_object(Bucket=cache_s3_access.bucket_name, Key=s3_key, Body=content, ChecksumAlgorithm="CRC64NVME")
    s3_object_metadata = cache_s3_access.get_s3_object_metadata(s3_key)
    assert s3_object_metadata.get_content_sha512() is None
    dest_path = Path(tmp_path, "no_sha512.txt")
    assert not cache_s3_access.download_cached(s3_key, dest_path).cache_hit
    index_path = Path(cache_s3_access.cache_dir, cache_index_dir_name, s3_object_metadata.get_identity_sha512())
    assert index_path.read_text() == hashlib.sha512(content).hexdigest()
    assert cache_s3_access.download_cached(s3_key, dest_path).cache_hit
    assert dest_path.read_bytes() == content
    assert cache_s3_access.get_s3_object_metadata(s3_key) == s3_object_metadata

    index_path.unlink()  # without the index file the SHA-512 is not known, so it is downloaded
    assert not cache_s3_access.download_cached(s3_key, dest_path).cache_hit
