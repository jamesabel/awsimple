"""
Bring a local directory up to date with an S3 bucket (issue 54, item 4).

All tests, fixtures, helpers and constants in this file are AI-generated (Claude Code).
"""

import os
from pathlib import Path
from threading import Event

import pytest
from boto3.s3.transfer import TransferConfig

from awsimple import BucketNotFound, S3Access, S3SyncOutcome, S3SyncResult
from awsimple.s3 import case_insensitive_file_names
from test_awsimple import test_awsimple_str

sync_prefix = "sync_to_directory/"


def _write(s3_access: S3Access, prefix: str, contents: dict):
    for name, content in contents.items():
        s3_access.write_string(content, f"{prefix}{name}")


def _outcomes(result: S3SyncResult, prefix: str) -> dict:
    return {s3_key[len(prefix) :]: sync_object.outcome for s3_key, sync_object in result.objects.items()}


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_sync_to_directory(s3_access, tmp_path, monkeypatch):
    prefix = f"{sync_prefix}basic/"
    contents = {"a.txt": "a", "sub/b.txt": "bb", "sub/deeper/c.txt": "ccc", "empty.txt": ""}
    _write(s3_access, prefix, contents)
    dest_dir = Path(tmp_path, "dest")  # does not exist yet

    metadata_requests = []
    get_s3_object_metadata = s3_access.get_s3_object_metadata
    head_object = s3_access.client.head_object
    head_requests = []

    def _get_s3_object_metadata(s3_key):
        metadata_requests.append(s3_key)
        return get_s3_object_metadata(s3_key)

    def _head_object(**kwargs):
        head_requests.append(kwargs["Key"])
        return head_object(**kwargs)

    monkeypatch.setattr(s3_access, "get_s3_object_metadata", _get_s3_object_metadata)
    monkeypatch.setattr(s3_access.client, "head_object", _head_object)

    result = s3_access.sync_to_directory(dest_dir, prefix)
    assert _outcomes(result, prefix) == {name: S3SyncOutcome.downloaded for name in contents}
    assert not result.stopped
    assert sorted(metadata_requests) == sorted(f"{prefix}{name}" for name in contents)  # one metadata request per downloaded object ...
    assert sorted(head_requests) == sorted(metadata_requests)  # ... and none by the transfer manager
    assert result.get_count() == len(contents)
    assert result.get_size() == result.get_size(S3SyncOutcome.downloaded) == sum(len(content) for content in contents.values())
    for name, content in contents.items():
        local_path = dest_dir.joinpath(*name.split("/"))  # the prefix is not part of the local path
        assert local_path.read_text() == content
        assert result.objects[f"{prefix}{name}"].local_path == local_path
        assert local_path.stat().st_mtime == pytest.approx(get_s3_object_metadata(f"{prefix}{name}").mtime.timestamp(), abs=1.0)
    assert [path for path in dest_dir.rglob("*") if path.name.endswith(".tmp")] == []  # no temporary file is left

    # everything is current, so a second sync makes no request per object
    metadata_requests.clear()
    result = s3_access.sync_to_directory(dest_dir, prefix)
    assert _outcomes(result, prefix) == {name: S3SyncOutcome.up_to_date for name in contents}
    assert metadata_requests == []


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_sync_to_directory_downloads_what_is_not_current(s3_access, tmp_path):
    prefix = f"{sync_prefix}not_current/"
    contents = {"missing.txt": "missing", "size.txt": "the S3 size", "older.txt": "same size", "newer.txt": "same size", "current.txt": "current"}
    _write(s3_access, prefix, contents)
    assert s3_access.sync_to_directory(tmp_path, prefix).get_count(S3SyncOutcome.downloaded) == len(contents)

    s3_mtime = s3_access.get_s3_object_metadata(f"{prefix}older.txt").mtime.timestamp()
    Path(tmp_path, "missing.txt").unlink()
    Path(tmp_path, "size.txt").write_text("a different local size")
    Path(tmp_path, "older.txt").write_text("SAME SIZE")
    os.utime(Path(tmp_path, "older.txt"), (s3_mtime - 3600.0, s3_mtime - 3600.0))  # the S3 object is newer
    Path(tmp_path, "newer.txt").write_text("SAME SIZE")
    os.utime(Path(tmp_path, "newer.txt"), (s3_mtime + 3600.0, s3_mtime + 3600.0))  # the local file is newer, so it is kept
    Path(tmp_path, "local_only.txt").write_text("not in the bucket")

    expected = {"missing.txt": S3SyncOutcome.would_download, "size.txt": S3SyncOutcome.would_download, "older.txt": S3SyncOutcome.would_download}
    expected |= {"newer.txt": S3SyncOutcome.up_to_date, "current.txt": S3SyncOutcome.up_to_date}

    # a dry run reports what a sync downloads and changes nothing
    result = s3_access.sync_to_directory(tmp_path, prefix, dry_run=True)
    assert _outcomes(result, prefix) == expected
    assert not Path(tmp_path, "missing.txt").exists()
    assert Path(tmp_path, "older.txt").read_text() == "SAME SIZE"

    # with size only, the modification times are not compared
    result = s3_access.sync_to_directory(tmp_path, prefix, size_only=True, dry_run=True)
    assert _outcomes(result, prefix) == expected | {"older.txt": S3SyncOutcome.up_to_date}

    result = s3_access.sync_to_directory(tmp_path, prefix)
    assert _outcomes(result, prefix) == {name: S3SyncOutcome.downloaded if outcome == S3SyncOutcome.would_download else outcome for name, outcome in expected.items()}
    for name in ("missing.txt", "size.txt", "older.txt", "current.txt"):
        assert Path(tmp_path, name).read_text() == contents[name]
    assert Path(tmp_path, "newer.txt").read_text() == "SAME SIZE"
    assert Path(tmp_path, "local_only.txt").read_text() == "not in the bucket"  # local files are never deleted


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_sync_to_directory_dry_run_makes_no_directory(s3_access, tmp_path):
    prefix = f"{sync_prefix}dry_run/"
    _write(s3_access, prefix, {"a.txt": "a"})
    dest_dir = Path(tmp_path, "dest")
    result = s3_access.sync_to_directory(dest_dir, prefix, dry_run=True)
    assert _outcomes(result, prefix) == {"a.txt": S3SyncOutcome.would_download}
    assert not dest_dir.exists()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_sync_to_directory_skips(s3_access, tmp_path):
    prefix = f"{sync_prefix}skips/"
    _write(s3_access, prefix, {"ok.txt": "ok", "same/file.txt": "one", "same//file.txt": "two", "both": "a file", "both/under.txt": "under", "parent/../escape.txt": "escape"})
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=f"{prefix}folder/", Body=b"")
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=f"{prefix}folder_with_content/", Body=b"not empty")
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=f"{prefix}glacier.txt", Body=b"archived", StorageClass="GLACIER")
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=f"{prefix}deep_archive.txt", Body=b"archived", StorageClass="DEEP_ARCHIVE")

    result = s3_access.sync_to_directory(tmp_path, prefix)
    assert _outcomes(result, prefix) == {
        "ok.txt": S3SyncOutcome.downloaded,
        "same/file.txt": S3SyncOutcome.skipped,  # every key that is the same local file is skipped, since none of them is the one
        "same//file.txt": S3SyncOutcome.skipped,
        "both": S3SyncOutcome.skipped,  # a file where another key needs a directory
        "both/under.txt": S3SyncOutcome.downloaded,
        "parent/../escape.txt": S3SyncOutcome.failed,  # can not be a file under the destination directory
        "folder/": S3SyncOutcome.skipped,
        "folder_with_content/": S3SyncOutcome.skipped,
        "glacier.txt": S3SyncOutcome.skipped,
        "deep_archive.txt": S3SyncOutcome.skipped,
    }
    for sync_object in result.get_objects(S3SyncOutcome.skipped) + result.get_objects(S3SyncOutcome.failed):
        assert len(sync_object.reason) > 0
    assert result.objects[f"{prefix}same/file.txt"].local_path == result.objects[f"{prefix}same//file.txt"].local_path == Path(tmp_path, "same", "file.txt")
    assert result.objects[f"{prefix}parent/../escape.txt"].local_path is None
    assert sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*") if path.is_file()) == ["both/under.txt", "ok.txt"]
    assert not Path(tmp_path.parent, "escape.txt").exists()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_sync_to_directory_keys_that_differ_in_case(s3_access, tmp_path):
    prefix = f"{sync_prefix}case/"
    _write(s3_access, prefix, {"Name.txt": "upper", "name.txt": "lower"})
    result = s3_access.sync_to_directory(tmp_path, prefix)
    if case_insensitive_file_names:
        # the same local file on Windows and macOS
        assert _outcomes(result, prefix) == {"Name.txt": S3SyncOutcome.skipped, "name.txt": S3SyncOutcome.skipped}
        assert list(tmp_path.iterdir()) == []
    else:
        assert _outcomes(result, prefix) == {"Name.txt": S3SyncOutcome.downloaded, "name.txt": S3SyncOutcome.downloaded}
        assert Path(tmp_path, "Name.txt").read_text() == "upper"
        assert Path(tmp_path, "name.txt").read_text() == "lower"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_sync_to_directory_failure_does_not_stop_the_others(s3_access, tmp_path):
    prefix = f"{sync_prefix}failure/"
    _write(s3_access, prefix, {"blocked.txt": "the S3 contents", "ok.txt": "ok"})
    Path(tmp_path, "blocked.txt").mkdir()  # a local directory is where the file has to be
    result = s3_access.sync_to_directory(tmp_path, prefix)
    assert _outcomes(result, prefix) == {"blocked.txt": S3SyncOutcome.failed, "ok.txt": S3SyncOutcome.downloaded}
    assert len(result.objects[f"{prefix}blocked.txt"].reason) > 0
    assert Path(tmp_path, "blocked.txt").is_dir()
    assert Path(tmp_path, "ok.txt").read_text() == "ok"
    assert [path for path in tmp_path.rglob("*") if path.name.endswith(".tmp")] == []


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_sync_to_directory_keeps_the_local_file_when_the_check_fails(s3_access, tmp_path, monkeypatch):
    # what was downloaded does not have the CRC64NVME of the S3 object: the object is failed and the local file stays as it was
    prefix = f"{sync_prefix}check_fails/"
    _write(s3_access, prefix, {"a.txt": "the S3 contents"})
    local_path = Path(tmp_path, "a.txt")
    local_path.write_text("local")
    get_s3_object_metadata = s3_access.get_s3_object_metadata

    def _wrong_crc(s3_key):
        s3_object_metadata = get_s3_object_metadata(s3_key)
        s3_object_metadata.crc64nvme = "0" * 16
        return s3_object_metadata

    monkeypatch.setattr(s3_access, "get_s3_object_metadata", _wrong_crc)
    result = s3_access.sync_to_directory(tmp_path, prefix)
    assert _outcomes(result, prefix) == {"a.txt": S3SyncOutcome.failed}
    assert "CRC64NVME" in result.objects[f"{prefix}a.txt"].reason
    assert local_path.read_text() == "local"
    assert list(tmp_path.iterdir()) == [local_path]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_sync_to_directory_callback_and_stop(tmp_path, monkeypatch):
    class SingleThreadDownload(S3Access):
        # one download at a time, so that what a stop does is the same every time
        def get_s3_download_transfer_config(self) -> TransferConfig:
            return TransferConfig(use_threads=False)

    s3_access = SingleThreadDownload(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)
    prefix = f"{sync_prefix}stop/"
    contents = {"a.txt": "a", "b.txt": "bb", "c.txt": "ccc", "folder/": ""}
    _write(s3_access, prefix, contents)

    # the callback gets every object, in a dry run too
    called = []
    result = s3_access.sync_to_directory(tmp_path, prefix, dry_run=True, callback=called.append)
    assert called == list(result.objects.values())

    # A stop is requested during the first download. It is requested from the thread that downloads (with its metadata request), not from the callback: the callback
    # is called from this thread, by when the one downloading thread can have downloaded every object.
    stop_event = Event()
    get_s3_object_metadata = s3_access.get_s3_object_metadata

    def _get_s3_object_metadata_and_stop(s3_key):
        stop_event.set()
        return get_s3_object_metadata(s3_key)

    called = []
    with monkeypatch.context() as patch:
        patch.setattr(s3_access, "get_s3_object_metadata", _get_s3_object_metadata_and_stop)
        result = s3_access.sync_to_directory(tmp_path, prefix, stop_event=stop_event, callback=called.append)
    assert result.stopped
    assert sorted(called, key=lambda sync_object: sync_object.key) == list(result.objects.values())
    assert result.get_count(S3SyncOutcome.skipped) == 1
    assert result.get_count(S3SyncOutcome.downloaded) == 1  # the download that had started is completed
    assert result.get_count(S3SyncOutcome.not_attempted) == 2
    assert len([path for path in tmp_path.iterdir() if path.is_file()]) == 1

    # the next sync downloads the others
    result = s3_access.sync_to_directory(tmp_path, prefix)
    assert not result.stopped
    assert result.get_count(S3SyncOutcome.not_attempted) == 0
    assert sorted(path.name for path in tmp_path.iterdir()) == ["a.txt", "b.txt", "c.txt"]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_sync_to_directory_multipart(s3_access, tmp_path):
    # larger than the multipart threshold, so the transfer manager downloads it in ranges, with the size awsimple gave it
    prefix = f"{sync_prefix}multipart/"
    content = os.urandom(9 * 1024 * 1024)
    source_path = Path(tmp_path, "source.bin")
    source_path.write_bytes(content)
    s3_access.upload(source_path, f"{prefix}large.bin")
    dest_dir = Path(tmp_path, "dest")
    result = s3_access.sync_to_directory(dest_dir, prefix)
    assert _outcomes(result, prefix) == {"large.bin": S3SyncOutcome.downloaded}
    assert Path(dest_dir, "large.bin").read_bytes() == content


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_sync_to_directory_bucket_not_found(tmp_path):
    with pytest.raises(BucketNotFound):
        S3Access(profile_name=test_awsimple_str, bucket_name="doesnotexist").sync_to_directory(tmp_path)
