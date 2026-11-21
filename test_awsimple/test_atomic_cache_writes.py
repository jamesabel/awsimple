"""
Cache writes are atomic: a cache file is either absent, its previous contents, or all of its new contents - never part of them.

All tests, helpers and constants in this file are AI-generated (Claude Code).
"""

import pickle
from pathlib import Path

import pytest

from awsimple import DynamoDBAccess, atomic_write_path, cache, dynamodb, lru_cache_write
from test_awsimple import temp_dir, test_awsimple_str


def _file_names(directory: Path) -> set:
    return {path.name for path in directory.rglob("*") if path.is_file()}


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_atomic_write_path_replaces_destination(tmp_path):
    dest_path = Path(tmp_path, "dest.bin")
    dest_path.write_bytes(b"previous contents")
    with atomic_write_path(dest_path) as temp_path:
        assert temp_path.parent == dest_path.parent  # the same directory, so the replace is on one file system
        temp_path.write_bytes(b"new")
        assert dest_path.read_bytes() == b"previous contents"  # not changed until all of the new contents are written
    assert dest_path.read_bytes() == b"new"
    assert _file_names(tmp_path) == {"dest.bin"}  # the temporary file is gone


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_atomic_write_path_failure_leaves_destination_unchanged(tmp_path):
    dest_path = Path(tmp_path, "dest.bin")
    dest_path.write_bytes(b"previous contents")
    with pytest.raises(OSError):
        with atomic_write_path(dest_path) as temp_path:
            temp_path.write_bytes(b"part of the new")
            raise OSError("the write was interrupted")
    assert dest_path.read_bytes() == b"previous contents"
    assert _file_names(tmp_path) == {"dest.bin"}


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_lru_cache_write_leaves_no_temporary_file(tmp_path):
    cache_dir = Path(tmp_path, "cache")
    source_path = Path(tmp_path, "source.bin")
    source_path.write_bytes(b"from a file")
    assert lru_cache_write(source_path, cache_dir, "from_file")
    assert lru_cache_write(b"from bytes", cache_dir, "from_bytes")
    assert _file_names(cache_dir) == {"from_file", "from_bytes"}


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_lru_cache_write_interrupted_leaves_existing_entry(tmp_path, monkeypatch):
    cache_dir = Path(tmp_path, "cache")
    assert lru_cache_write(b"previous contents", cache_dir, "entry")
    source_path = Path(tmp_path, "source.bin")
    source_path.write_bytes(b"new contents")

    def _interrupted_copy(_unused_source, destination):
        Path(destination).write_bytes(b"new co")  # part of the new contents
        raise OSError("the copy was interrupted")

    monkeypatch.setattr(cache, "copy2", _interrupted_copy)
    assert not lru_cache_write(source_path, cache_dir, "entry")
    assert Path(cache_dir, "entry").read_bytes() == b"previous contents"
    assert _file_names(cache_dir) == {"entry"}


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_dynamodb_scan_cache_interrupted_write_leaves_no_cache_file(tmp_path, monkeypatch):
    cache_dir = Path(tmp_path, "cache")
    dynamodb_access = DynamoDBAccess(f"{test_awsimple_str}atomic", profile_name=test_awsimple_str, cache_dir=cache_dir)
    dynamodb_access.create_table("id")
    try:
        dynamodb_access.put_item({"id": "me", "value": 1})
        assert len(dynamodb_access.scan_table_cached()) == 1
        cache_file_path = dynamodb_access.get_cache_file_path()
        assert _file_names(cache_dir) == {cache_file_path.name}

        def _interrupted_dump(_unused_object, file):
            file.write(b"part of a pickle")
            raise pickle.PicklingError("the write was interrupted")

        with monkeypatch.context() as patch:
            patch.setattr(dynamodb.pickle, "dump", _interrupted_dump)
            with pytest.raises(pickle.PicklingError):
                dynamodb_access.scan_table_cached(invalidate_cache=True)
        assert _file_names(cache_dir) == set()  # no cache file with part of a pickle in it, and no temporary file

        assert len(dynamodb_access.scan_table_cached()) == 1  # and the next scan writes the cache file
        assert _file_names(cache_dir) == {cache_file_path.name}
    finally:
        dynamodb_access.delete_table()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cached_download_leaves_no_temporary_file(s3_access):
    s3_key = "atomic_cache_write.bin"
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=b"atomic cache write", ChecksumAlgorithm="CRC64NVME")  # no SHA-512, so the cache index is written too
    dest_path = Path(temp_dir, s3_key)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    assert s3_access.download_cached(s3_key, dest_path).cache_write
    assert s3_access.download_cached(s3_key, dest_path).cache_hit
    assert not [name for name in _file_names(s3_access.cache_dir) if name.endswith(cache.atomic_write_temp_suffix)]
