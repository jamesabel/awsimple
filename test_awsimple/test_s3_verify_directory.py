"""
Verify the files of a local directory against an S3 bucket: S3Access.verify_directory() and the parts of it that do not access S3 (awsimple/s3_verify.py).

All tests, fixtures, helpers and constants in this file are AI-generated (Claude Code).
"""

import base64
import hashlib
import os
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event
from typing import Union

import pytest
from awscrt import checksums
from boto3.s3.transfer import TransferConfig

from awsimple import (
    AWSimpleException,
    BucketNotFound,
    S3Access,
    S3ObjectMetadata,
    S3SyncObject,
    S3SyncOutcome,
    S3VerifyObject,
    S3VerifyOutcome,
    S3VerifyResult,
    get_file_crc64nvme,
    get_s3_object_hash,
)
from awsimple.s3_verify import plan_verify, verify_local_file
from test_awsimple import test_awsimple_str

verify_prefix = "verify_directory/"

s3_mtime = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)
file_contents = b"the contents of the S3 object"


def _write(s3_access: S3Access, prefix: str, contents: dict):
    for name, content in contents.items():
        s3_access.write_string(content, f"{prefix}{name}")


def _outcomes(result: S3VerifyResult, prefix: str) -> dict:
    return {s3_key[len(prefix) :]: verify_object.outcome for s3_key, verify_object in result.objects.items()}


def _set_contents_keeping_mtime(path: Path, contents: str):
    # what a sync does not see: the size and the modification time stay what they were
    file_stat = path.stat()
    path.write_text(contents)
    os.utime(path, (file_stat.st_mtime, file_stat.st_mtime))


def _to_base64(hex_string: str) -> str:
    # an S3 native checksum as S3 gives it
    return base64.b64encode(bytes.fromhex(hex_string)).decode()


def _make_metadata(metadata_sha512: Union[str, None] = None, native_checksums: Union[dict, None] = None, size: int = len(file_contents)) -> S3ObjectMetadata:
    native_checksums = {} if native_checksums is None else native_checksums
    return S3ObjectMetadata("bucket", "a.txt", size, s3_mtime, "etag", None, "url", metadata_sha512, None, None, "FULL_OBJECT" if native_checksums else None, native_checksums)


def _make_local_file(tmp_path: Path, contents: bytes = file_contents, mtime: datetime = s3_mtime) -> S3VerifyObject:
    local_path = Path(tmp_path, "a.txt")
    local_path.write_bytes(contents)
    os.utime(local_path, (mtime.timestamp(), mtime.timestamp()))
    return S3VerifyObject("a.txt", S3VerifyOutcome.not_attempted, local_path)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_verify_directory(s3_access, tmp_path, monkeypatch):
    prefix = f"{verify_prefix}basic/"
    contents = {"a.txt": "a", "sub/b.txt": "bb", "sub/deeper/c.txt": "ccc", "empty.txt": ""}
    _write(s3_access, prefix, contents)
    sync_result = s3_access.sync_to_directory(tmp_path, prefix)
    files_before = {path: (path.read_bytes(), path.stat().st_mtime) for path in tmp_path.rglob("*") if path.is_file()}

    metadata_requests = []
    get_s3_object_metadata = s3_access.get_s3_object_metadata

    def _get_s3_object_metadata(s3_key):
        metadata_requests.append(s3_key)
        return get_s3_object_metadata(s3_key)

    monkeypatch.setattr(s3_access, "get_s3_object_metadata", _get_s3_object_metadata)

    # with the result of the sync (no listing) and without it (a listing): the same
    for given_sync_result in (sync_result, None):
        result = s3_access.verify_directory(tmp_path, prefix, sync_result=given_sync_result)
        assert _outcomes(result, prefix) == {name: S3VerifyOutcome.verified for name in contents}
        assert not result.stopped
        assert result.get_count() == result.get_count(S3VerifyOutcome.verified) == len(contents)
        for name in contents:
            verify_object = result.objects[f"{prefix}{name}"]
            assert verify_object.local_path == tmp_path.joinpath(*name.split("/"))
            assert verify_object.hash_name == "SHA-512"  # awsimple writes every object with the SHA-512 of its contents
            assert verify_object.reason == ""
        assert sorted(metadata_requests) == sorted(f"{prefix}{name}" for name in contents)  # one metadata request per object
        metadata_requests.clear()

    # nothing was changed
    assert {path: (path.read_bytes(), path.stat().st_mtime) for path in tmp_path.rglob("*") if path.is_file()} == files_before


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_verify_directory_finds_what_differs(s3_access, tmp_path):
    assert s3_access.mtime_abs_tol == 10.0  # modification times within this window are the same
    prefix = f"{verify_prefix}differs/"
    names = ["same.txt", "damaged.txt", "missing.txt", "size.txt", "mtime.txt", "mtime_in_window.txt"]
    _write(s3_access, prefix, {name: "contents" for name in names})
    s3_access.sync_to_directory(tmp_path, prefix)

    _set_contents_keeping_mtime(Path(tmp_path, "damaged.txt"), "CONTENTS")
    Path(tmp_path, "missing.txt").unlink()
    _set_contents_keeping_mtime(Path(tmp_path, "size.txt"), "another size")
    s3_mtime_timestamp = s3_access.get_s3_object_metadata(f"{prefix}mtime.txt").mtime.timestamp()
    os.utime(Path(tmp_path, "mtime.txt"), (s3_mtime_timestamp + 3600.0, s3_mtime_timestamp + 3600.0))
    os.utime(Path(tmp_path, "mtime_in_window.txt"), (s3_mtime_timestamp + 8.0, s3_mtime_timestamp + 8.0))

    result = s3_access.verify_directory(tmp_path, prefix)

    assert _outcomes(result, prefix) == {
        "same.txt": S3VerifyOutcome.verified,
        "damaged.txt": S3VerifyOutcome.differs,
        "missing.txt": S3VerifyOutcome.differs,
        "size.txt": S3VerifyOutcome.differs,
        "mtime.txt": S3VerifyOutcome.mtime_differs,
        "mtime_in_window.txt": S3VerifyOutcome.verified,
    }
    sha512 = hashlib.sha512(b"contents").hexdigest()
    assert result.objects[f"{prefix}damaged.txt"].reason == f"the SHA-512 of the local file is {hashlib.sha512(b'CONTENTS').hexdigest()} but the S3 object's is {sha512}"
    assert result.objects[f"{prefix}missing.txt"].reason.startswith(f'no local file "{Path(tmp_path, "missing.txt")}"')
    assert result.objects[f"{prefix}size.txt"].reason == "the local file is 12 bytes but the S3 object is 8 bytes"
    assert result.objects[f"{prefix}size.txt"].hash_name is None  # the hash would only say it again
    assert result.objects[f"{prefix}mtime.txt"].reason.startswith("the local file was modified +3600 seconds from the S3 object (")
    assert result.objects[f"{prefix}mtime.txt"].hash_name == "SHA-512"  # its contents are the S3 object's
    assert result.get_count(S3VerifyOutcome.differs) == 3
    assert Path(tmp_path, "damaged.txt").read_text() == "CONTENTS"  # a verification changes nothing


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_verify_directory_native_checksums(s3_access, tmp_path):
    # objects that were not written by awsimple, each with one of S3's native full-object checksums and no SHA-512
    prefix = f"{verify_prefix}native/"
    hash_names = {"SHA256": "SHA-256", "SHA1": "SHA-1", "CRC64NVME": "CRC64NVME", "CRC32C": "CRC32C", "CRC32": "CRC32"}
    for checksum_algorithm in hash_names:
        s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=f"{prefix}{checksum_algorithm}.bin", Body=b"native checksum", ChecksumAlgorithm=checksum_algorithm)
    sync_result = s3_access.sync_to_directory(tmp_path, prefix)
    assert sync_result.get_count(S3SyncOutcome.downloaded) == len(hash_names)

    result = s3_access.verify_directory(tmp_path, prefix, sync_result=sync_result)
    assert {s3_key[len(prefix) : -len(".bin")]: (verify_object.outcome, verify_object.hash_name) for s3_key, verify_object in result.objects.items()} == {
        checksum_algorithm: (S3VerifyOutcome.verified, hash_name) for checksum_algorithm, hash_name in hash_names.items()
    }

    # every one of them finds a file that was damaged
    for checksum_algorithm in hash_names:
        _set_contents_keeping_mtime(Path(tmp_path, f"{checksum_algorithm}.bin"), "NATIVE CHECKSUM")
    result = s3_access.verify_directory(tmp_path, prefix, sync_result=sync_result)
    assert result.get_count(S3VerifyOutcome.differs) == len(hash_names)
    for checksum_algorithm, hash_name in hash_names.items():
        assert result.objects[f"{prefix}{checksum_algorithm}.bin"].reason.startswith(f"the {hash_name} of the local file is ")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_verify_directory_skips_what_has_no_local_file(s3_access, tmp_path):
    prefix = f"{verify_prefix}skips/"
    _write(s3_access, prefix, {"a.txt": "a"})
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=f"{prefix}folder/", Body=b"")
    called = []

    result = s3_access.verify_directory(tmp_path, prefix, callback=called.append)  # nothing was synced

    assert _outcomes(result, prefix) == {"a.txt": S3VerifyOutcome.differs, "folder/": S3VerifyOutcome.skipped}
    assert result.objects[f"{prefix}folder/"].reason.startswith("sync outcome is skipped : ")
    assert sorted(called, key=lambda verify_object: verify_object.key) == list(result.objects.values())  # the callback gets every object

    # with the result of a sync, only what that sync made (or found) a local file for is verified: a dry run makes none
    dry_run_result = s3_access.sync_to_directory(tmp_path, prefix, dry_run=True)
    result = s3_access.verify_directory(tmp_path, prefix, sync_result=dry_run_result)
    assert _outcomes(result, prefix) == {"a.txt": S3VerifyOutcome.skipped, "folder/": S3VerifyOutcome.skipped}
    assert result.objects[f"{prefix}a.txt"].reason == "sync outcome is would download"
    assert not result.stopped


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_verify_directory_failure_does_not_stop_the_others(s3_access, tmp_path, monkeypatch):
    prefix = f"{verify_prefix}failure/"
    _write(s3_access, prefix, {"a.txt": "a", "gone.txt": "gone", "c.txt": "c"})
    sync_result = s3_access.sync_to_directory(tmp_path, prefix)
    get_s3_object_metadata = s3_access.get_s3_object_metadata

    def _get_s3_object_metadata(s3_key):
        if s3_key.endswith("gone.txt"):
            raise AWSimpleException(f"{s3_key=} does not exist")  # e.g. it was deleted since the sync
        return get_s3_object_metadata(s3_key)

    monkeypatch.setattr(s3_access, "get_s3_object_metadata", _get_s3_object_metadata)

    result = s3_access.verify_directory(tmp_path, prefix, sync_result=sync_result)

    assert _outcomes(result, prefix) == {"a.txt": S3VerifyOutcome.verified, "gone.txt": S3VerifyOutcome.failed, "c.txt": S3VerifyOutcome.verified}
    assert result.objects[f"{prefix}gone.txt"].reason.startswith("could not get the metadata of the S3 object (")
    assert not result.stopped


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_verify_directory_callback_and_stop(tmp_path, monkeypatch):
    class SingleThreadDownload(S3Access):
        # one object at a time, so that what a stop does is the same every time
        def get_s3_download_transfer_config(self) -> TransferConfig:
            return TransferConfig(use_threads=False)

    s3_access = SingleThreadDownload(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)
    prefix = f"{verify_prefix}stop/"
    _write(s3_access, prefix, {"a.txt": "a", "b.txt": "bb", "c.txt": "ccc"})
    sync_result = s3_access.sync_to_directory(tmp_path, prefix)

    # A stop is requested while the first object is verified. It is requested from the thread that verifies (with its metadata request), not from the callback: the
    # callback is called from this thread, by when the one verifying thread can have verified every object.
    stop_event = Event()
    get_s3_object_metadata = s3_access.get_s3_object_metadata

    def _get_s3_object_metadata_and_stop(s3_key):
        stop_event.set()
        return get_s3_object_metadata(s3_key)

    monkeypatch.setattr(s3_access, "get_s3_object_metadata", _get_s3_object_metadata_and_stop)
    called = []

    result = s3_access.verify_directory(tmp_path, prefix, sync_result=sync_result, stop_event=stop_event, callback=called.append)

    assert result.stopped
    assert sorted(called, key=lambda verify_object: verify_object.key) == list(result.objects.values())
    assert result.get_count(S3VerifyOutcome.verified) == 1  # what had started is completed
    assert result.get_count(S3VerifyOutcome.not_attempted) == 2


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_verify_directory_bucket_not_found(tmp_path):
    with pytest.raises(BucketNotFound):
        S3Access(profile_name=test_awsimple_str, bucket_name="doesnotexist").verify_directory(tmp_path)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_get_s3_object_hash_is_the_strongest_that_s3_has():
    sha512 = hashlib.sha512(file_contents).hexdigest()
    sha256 = _to_base64(hashlib.sha256(file_contents).hexdigest())
    crc32 = _to_base64(format(zlib.crc32(file_contents), "08x"))

    s3_object_hash = get_s3_object_hash(_make_metadata(sha512, {"ChecksumSHA256": sha256, "ChecksumCRC32": crc32}))
    assert (s3_object_hash.name, s3_object_hash.value) == ("SHA-512", sha512)
    s3_object_hash = get_s3_object_hash(_make_metadata(None, {"ChecksumSHA256": sha256, "ChecksumCRC32": crc32}))
    assert (s3_object_hash.name, s3_object_hash.value) == ("SHA-256", hashlib.sha256(file_contents).hexdigest())
    assert get_s3_object_hash(_make_metadata(None, {"ChecksumCRC32": crc32})).name == "CRC32"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.parametrize(
    "hash_name,checksum_name,hex_hash",
    [
        ("SHA-256", "ChecksumSHA256", hashlib.sha256(file_contents).hexdigest()),
        ("SHA-1", "ChecksumSHA1", hashlib.sha1(file_contents).hexdigest()),
        ("CRC32C", "ChecksumCRC32C", format(checksums.crc32c(file_contents), "08x")),
        ("CRC32", "ChecksumCRC32", format(zlib.crc32(file_contents), "08x")),
    ],
)
def test_verify_local_file_with_each_native_checksum(tmp_path, hash_name, checksum_name, hex_hash):
    verify_object = _make_local_file(tmp_path)
    verify_local_file(verify_object, _make_metadata(None, {checksum_name: _to_base64(hex_hash)}), 10.0)
    assert (verify_object.outcome, verify_object.hash_name) == (S3VerifyOutcome.verified, hash_name)

    other_hash = "00" * (len(hex_hash) // 2)
    verify_object = _make_local_file(tmp_path)
    verify_local_file(verify_object, _make_metadata(None, {checksum_name: _to_base64(other_hash)}), 10.0)
    assert verify_object.outcome == S3VerifyOutcome.differs
    assert verify_object.reason == f"the {hash_name} of the local file is {hex_hash} but the S3 object's is {other_hash}"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_verify_local_file_with_crc64nvme(tmp_path):
    verify_object = _make_local_file(tmp_path)
    assert verify_object.local_path is not None
    crc64nvme = get_file_crc64nvme(verify_object.local_path)

    verify_local_file(verify_object, _make_metadata(None, {"ChecksumCRC64NVME": _to_base64(crc64nvme)}), 10.0)

    assert (verify_object.outcome, verify_object.hash_name) == (S3VerifyOutcome.verified, "CRC64NVME")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_verify_local_file_of_an_object_without_a_hash_of_its_whole_contents(tmp_path):
    # S3 has no checksum of the object, or a composite one of its parts ("-<number of parts>"), which is not a hash of the file
    composite = {"ChecksumSHA256": f"{_to_base64(hashlib.sha256(file_contents).hexdigest())}-3"}
    for s3_object_metadata in (_make_metadata(), _make_metadata(None, composite)):
        assert get_s3_object_hash(s3_object_metadata) is None
        verify_object = _make_local_file(tmp_path)
        verify_local_file(verify_object, s3_object_metadata, 10.0)
        assert verify_object.outcome == S3VerifyOutcome.verified  # by its size and modification time
        assert verify_object.hash_name is None

    # ... which still finds another size or another modification time
    verify_object = _make_local_file(tmp_path, file_contents + b"more")
    verify_local_file(verify_object, _make_metadata(), 10.0)
    assert verify_object.outcome == S3VerifyOutcome.differs
    verify_object = _make_local_file(tmp_path, mtime=s3_mtime - timedelta(seconds=11))
    verify_local_file(verify_object, _make_metadata(), 10.0)
    assert verify_object.outcome == S3VerifyOutcome.mtime_differs
    assert verify_object.reason == "the local file was modified -11 seconds from the S3 object (2026-11-20T12:00:00+00:00)"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.parametrize("seconds,outcome", [(-10, S3VerifyOutcome.verified), (10, S3VerifyOutcome.verified), (-11, S3VerifyOutcome.mtime_differs), (11, S3VerifyOutcome.mtime_differs)])
def test_verify_local_file_mtime_window(tmp_path, seconds, outcome):
    verify_object = _make_local_file(tmp_path, mtime=s3_mtime + timedelta(seconds=seconds))

    verify_local_file(verify_object, _make_metadata(hashlib.sha512(file_contents).hexdigest()), 10.0)

    assert verify_object.outcome == outcome


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_verify_local_file_that_can_not_be_read(tmp_path, monkeypatch):
    def _get_file_sha512(path):
        raise PermissionError("in use")

    monkeypatch.setattr("awsimple.s3_verify.get_file_sha512", _get_file_sha512)
    verify_object = _make_local_file(tmp_path)

    verify_local_file(verify_object, _make_metadata(hashlib.sha512(file_contents).hexdigest()), 10.0)

    assert verify_object.outcome == S3VerifyOutcome.failed  # it is not known to differ
    assert verify_object.reason == f'could not read the local file "{verify_object.local_path}" (in use)'


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_plan_verify(tmp_path):
    sync_objects = {
        "new.txt": S3SyncObject("new.txt", 1, S3SyncOutcome.downloaded, Path(tmp_path, "new.txt")),
        "old.txt": S3SyncObject("old.txt", 1, S3SyncOutcome.up_to_date, Path(tmp_path, "old.txt")),
        "folder/": S3SyncObject("folder/", 0, S3SyncOutcome.skipped, None, "a folder marker"),
        "broken.txt": S3SyncObject("broken.txt", 1, S3SyncOutcome.failed, Path(tmp_path, "broken.txt"), "connection lost"),
        "later.txt": S3SyncObject("later.txt", 1, S3SyncOutcome.not_attempted, Path(tmp_path, "later.txt")),
    }

    verify_objects = plan_verify(sync_objects, (S3SyncOutcome.downloaded, S3SyncOutcome.up_to_date))

    assert list(verify_objects) == list(sync_objects)  # in the order given
    assert {s3_key: (verify_object.outcome, verify_object.reason) for s3_key, verify_object in verify_objects.items()} == {
        "new.txt": (S3VerifyOutcome.not_attempted, ""),
        "old.txt": (S3VerifyOutcome.not_attempted, ""),
        "folder/": (S3VerifyOutcome.skipped, "sync outcome is skipped : a folder marker"),
        "broken.txt": (S3VerifyOutcome.skipped, "sync outcome is failed : connection lost"),
        "later.txt": (S3VerifyOutcome.skipped, "sync outcome is not attempted"),
    }
    assert verify_objects["new.txt"].local_path == Path(tmp_path, "new.txt")
