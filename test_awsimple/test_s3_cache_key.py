"""
Tests of the S3 object hash (SHA-512), the S3 cache that is based on it, and the length and CRC64NVME checks of S3 accesses.

All tests, helpers and constants in this file are AI-generated (Claude Code).
"""

import hashlib
import os
import string
from pathlib import Path

import pytest
from hashy import get_bytes_crc64nvme

from awsimple import AWSimpleException, S3Access, s3
from awsimple.s3 import sha512_string

from test_awsimple import temp_dir, test_awsimple_str

# two different contents with the same CRC64NVME (a CRC is linear, so 8 chosen trailing bytes give any content any CRC)
same_crc_content_a = b"genuine contents: pay 10 dollars to alice\n"
same_crc_content_b = b"different contents: pay 9999 dollars to mallory\n\x83\xc7?[\xaa\xad\x94m"

sha512_hex_length = 128


def _put(s3_access, s3_key: str, content: bytes):
    # a write by another tool: S3 validates the body against the SDK computed CRC64NVME, but there is no SHA-512 on the S3 object
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=content, ChecksumAlgorithm="CRC64NVME")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cache_key_is_a_sha512(s3_access):
    s3_key = "cache_key_is_a_sha512.txt"
    _put(s3_access, s3_key, b"cache key is a sha512")
    cache_key = s3_access.get_s3_object_metadata(s3_key).get_sha512()
    assert len(cache_key) == sha512_hex_length
    assert set(cache_key) <= set(string.hexdigits.lower())
    assert cache_key == s3_access.get_s3_object_metadata(s3_key).get_sha512()  # the same object has the same cache key every time


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cache_key_same_crc_different_objects(s3_access):
    # two objects with the same CRC64NVME must not share a cache key, or one is served from the cache in place of the other
    assert same_crc_content_a != same_crc_content_b
    assert get_bytes_crc64nvme(same_crc_content_a) == get_bytes_crc64nvme(same_crc_content_b)

    s3_key_a = "cache_key_same_crc_a.bin"
    s3_key_b = "cache_key_same_crc_b.bin"
    _put(s3_access, s3_key_a, same_crc_content_a)
    _put(s3_access, s3_key_b, same_crc_content_b)

    metadata_a = s3_access.get_s3_object_metadata(s3_key_a)
    metadata_b = s3_access.get_s3_object_metadata(s3_key_b)
    assert metadata_a.crc64nvme == metadata_b.crc64nvme
    assert metadata_a.get_sha512() != metadata_b.get_sha512()

    dest_a = Path(temp_dir, s3_key_a)
    dest_b = Path(temp_dir, s3_key_b)
    dest_a.parent.mkdir(parents=True, exist_ok=True)
    assert s3_access.download_cached(s3_key_a, dest_a).success
    assert s3_access.download_cached(s3_key_b, dest_b).success
    assert dest_a.read_bytes() == same_crc_content_a
    assert dest_b.read_bytes() == same_crc_content_b


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cache_key_changes_when_the_object_changes(s3_access):
    s3_key = "cache_key_changes.txt"
    _put(s3_access, s3_key, b"first contents")
    first_cache_key = s3_access.get_s3_object_metadata(s3_key).get_sha512()
    _put(s3_access, s3_key, b"other contents")  # the same size
    assert s3_access.get_s3_object_metadata(s3_key).get_sha512() != first_cache_key


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_written_objects_have_sha512(s3_access):
    # every way awsimple writes an S3 object also writes the SHA-512 of the contents, and that is the hash of the object
    s3_access.write_string("written as a string", "sha512_written_string.txt")
    metadata = s3_access.get_s3_object_metadata("sha512_written_string.txt")
    assert metadata.get_content_sha512() == hashlib.sha512(b"written as a string").hexdigest()
    assert metadata.get_sha512() == metadata.get_content_sha512()

    file_path = Path(temp_dir, "sha512_written_file.bin")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"written as a file")
    assert s3_access.upload(file_path, "sha512_written_file.bin", force=True)
    assert s3_access.get_s3_object_metadata("sha512_written_file.bin").get_content_sha512() == hashlib.sha512(b"written as a file").hexdigest()

    assert s3_access.upload_object_as_json({"written": "as json"}, "sha512_written_json", force=True)
    json_metadata = s3_access.get_s3_object_metadata("sha512_written_json.json")
    assert json_metadata.get_content_sha512() == hashlib.sha512(b'{"written": "as json"}').hexdigest()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cache_is_named_with_content_sha512(s3_access):
    # with or without a SHA-512 on the S3 object, the cached contents are in a file named with the SHA-512 of the contents
    content = b"cached by the sha512 of the contents"
    content_sha512 = hashlib.sha512(content).hexdigest()
    Path(s3_access.cache_dir, content_sha512).unlink(missing_ok=True)

    s3_key = "cache_named_no_sha512.bin"
    _put(s3_access, s3_key, content)  # no SHA-512 on the S3 object
    assert s3_access.get_s3_object_metadata(s3_key).get_content_sha512() is None
    dest = Path(temp_dir, s3_key)
    dest.parent.mkdir(parents=True, exist_ok=True)
    status = s3_access.download_cached(s3_key, dest)
    assert not status.cache_hit
    assert status.cache_write
    assert Path(s3_access.cache_dir, content_sha512).read_bytes() == content
    assert s3_access.download_cached(s3_key, dest).cache_hit  # found by the identity hash of the object
    assert dest.read_bytes() == content

    # the same contents written by awsimple (so the S3 object has the SHA-512) are the same cache file
    s3_key_with_sha512 = "cache_named_with_sha512.bin"
    dest.write_bytes(content)
    assert s3_access.upload(dest, s3_key_with_sha512, force=True)
    dest_with_sha512 = Path(temp_dir, s3_key_with_sha512)
    assert s3_access.download_cached(s3_key_with_sha512, dest_with_sha512).cache_hit
    assert dest_with_sha512.read_bytes() == content


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cache_hit_sets_mtime_and_leaves_s3_object_alone(s3_access):
    # an object without a SHA-512 is never given one by a read, and a file copied from the cache gets the mtime of the S3 object
    s3_key = "cache_hit_mtime.bin"
    content = b"cache hit mtime"
    _put(s3_access, s3_key, content)
    metadata_before = s3_access.get_s3_object_metadata(s3_key)
    dest = Path(temp_dir, s3_key)
    dest.parent.mkdir(parents=True, exist_ok=True)
    s3_access.download_cached(s3_key, dest)

    # as if the cached contents had been downloaded from another S3 object with the same contents and an older mtime
    os.utime(Path(s3_access.cache_dir, hashlib.sha512(content).hexdigest()), (0.0, 0.0))
    assert s3_access.download_cached(s3_key, dest).cache_hit
    assert os.path.getmtime(dest) == pytest.approx(metadata_before.mtime.timestamp(), abs=s3_access.mtime_abs_tol)

    assert s3_access.get_s3_object_metadata(s3_key) == metadata_before  # the reads did not change the S3 object
    assert metadata_before.get_content_sha512() is None


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cache_key_with_file_system_path_irregularities(s3_access):
    # keys that are irregular as file system paths (leading and doubled slashes, the same name in a different case, not allowed in a Windows file name) are separate in the cache
    contents = {
        "/irregular/key.bin": b"leading slash",
        "irregular//key.bin": b"doubled slash",
        "irregular/key.bin": b"plain",
        "irregular/KEY.bin": b"upper case",
        "irregular/a:b*c?.bin": b"not allowed",
    }
    for s3_key, content in contents.items():
        _put(s3_access, s3_key, content)
    assert len({s3_access.get_s3_object_metadata(s3_key).get_sha512() for s3_key in contents}) == len(contents)
    for number, (s3_key, content) in enumerate(contents.items()):
        dest = Path(temp_dir, f"irregular_key_{number}.bin")  # a full destination path, so the key is not part of it
        for _unused_pass in range(2):  # a cache miss, then a cache hit
            assert s3_access.download_cached(s3_key, dest).success
            assert dest.read_bytes() == content


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_wrong_sha512_metadata_is_not_cached(s3_access):
    # S3 does not validate object metadata: contents that do not have the SHA-512 the S3 object claims are never cached with that SHA-512
    claimed_content = b"the contents the metadata claims"
    claimed_sha512 = hashlib.sha512(claimed_content).hexdigest()
    Path(s3_access.cache_dir, claimed_sha512).unlink(missing_ok=True)
    s3_key = "wrong_sha512_metadata.bin"
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=b"some other contents", ChecksumAlgorithm="CRC64NVME", Metadata={sha512_string: claimed_sha512})
    with pytest.raises(AWSimpleException):
        s3_access.download_cached(s3_key, Path(temp_dir, s3_key))
    assert not Path(s3_access.cache_dir, claimed_sha512).exists()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_metadata_that_is_not_a_sha512_is_not_used(s3_access):
    # object metadata is whatever the writer set, so a value that is not a SHA-512 (e.g. a path) is never used as a cache file name
    s3_key = "not_a_sha512_metadata.bin"
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=b"contents", ChecksumAlgorithm="CRC64NVME", Metadata={sha512_string: "../not_a_sha512"})
    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.get_content_sha512() is None
    assert len(metadata.get_sha512()) == sha512_hex_length


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_upload_not_skipped_when_sha512_differs(s3_access):
    # the same CRC64NVME is not enough to skip an upload: the length and the SHA-512 must also be the same
    s3_key = "upload_same_crc.bin"
    file_path = Path(temp_dir, s3_key)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(same_crc_content_a)
    assert s3_access.upload(file_path, s3_key, force=True)
    assert not s3_access.upload(file_path, s3_key)  # unchanged
    file_path.write_bytes(same_crc_content_b)  # different contents with the same CRC64NVME
    assert s3_access.upload(file_path, s3_key)
    assert s3_access.get_s3_object_metadata(s3_key).get_content_sha512() == hashlib.sha512(same_crc_content_b).hexdigest()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_read_checks_length_and_crc64nvme(s3_access, monkeypatch):
    # what is read from S3 is checked against the length and the CRC64NVME of the S3 object
    s3_key = "read_checks.txt"
    s3_access.write_string("what was written", s3_key)
    assert s3_access.read_string(s3_key) == "what was written"

    monkeypatch.setattr(s3, "get_bytes_crc64nvme", lambda _unused_bytes: "0" * 16)  # as if the received bytes were corrupted
    with pytest.raises(AWSimpleException):
        s3_access.read_string(s3_key)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_write_checks_length_and_crc64nvme(s3_access, monkeypatch):
    # after a write, the S3 object is checked against the length and the CRC64NVME of what was written
    monkeypatch.setattr(s3, "get_bytes_crc64nvme", lambda _unused_bytes: "0" * 16)  # as if S3 stored something other than what was written
    with pytest.raises(AWSimpleException):
        s3_access.write_string("what was written", "write_checks.txt")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_download_checks_length_and_crc64nvme(s3_access, monkeypatch):
    # a downloaded file that does not have the length and the CRC64NVME of the S3 object is not a successful download
    s3_key = "download_checks.txt"
    s3_access.write_string("what was written", s3_key)
    dest = Path(temp_dir, s3_key)
    dest.parent.mkdir(parents=True, exist_ok=True)
    assert s3_access.download(s3_key, dest)
    assert dest.read_bytes() == b"what was written"

    retrying_s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str, cache_dir=s3_access.cache_dir)
    retrying_s3_access.retry_count = 2
    retrying_s3_access.retry_sleep_time = 0.0
    monkeypatch.setattr(s3, "get_file_crc64nvme", lambda _unused_path: "0" * 16)  # as if the downloaded file were corrupted
    with pytest.raises(AWSimpleException):
        retrying_s3_access.download(s3_key, dest)
