"""
An S3 key becomes a local path that is always under the destination directory (issue 54, item 2).

All tests, helpers and constants in this file are AI-generated (Claude Code).
"""

import os
from pathlib import Path

import pytest

from awsimple import AWSimpleException, s3_key_to_local_path
from test_awsimple import temp_dir

is_windows = os.name == "nt"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.parametrize(
    "s3_key,expected_components",
    [
        ("a.txt", ("a.txt",)),
        ("a/b/c.txt", ("a", "b", "c.txt")),
        ("/a.txt", ("a.txt",)),  # a leading slash does not go to the root of the drive or file system
        ("/temp/a.txt", ("temp", "a.txt")),
        ("a//b.txt", ("a", "b.txt")),  # empty components are dropped, as "aws s3 sync" does
        ("//server/share/a.txt", ("server", "share", "a.txt")),  # not a UNC path
        ("a/b/", ("a", "b")),
        ("...", ("...",)),  # only "." and ".." are special
    ],
)
def test_s3_key_to_local_path(tmp_path, s3_key, expected_components):
    local_path = s3_key_to_local_path(tmp_path, s3_key)
    assert local_path == tmp_path.joinpath(*expected_components)
    assert local_path.is_relative_to(tmp_path)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.parametrize("s3_key", ["", "/", "//", "..", "../a.txt", "a/../../b.txt", "a/..", "./a.txt", "a/./b.txt"])
def test_s3_key_to_local_path_rejects(tmp_path, s3_key):
    # no file name at all, or a component that refers to the current or parent directory
    with pytest.raises(AWSimpleException):
        s3_key_to_local_path(tmp_path, s3_key)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.skipif(not is_windows, reason="Windows path rules")
@pytest.mark.parametrize("s3_key", ["C:/a.txt", "C:a.txt", "a\\b.txt", "\\a.txt", "a/b.txt:stream", "a:b/c.txt"])
def test_s3_key_to_local_path_rejects_on_windows(tmp_path, s3_key):
    # a drive, a backslash (a path separator on Windows) or a colon (a drive or an alternate data stream) would put the file somewhere other than what the key says
    with pytest.raises(AWSimpleException):
        s3_key_to_local_path(tmp_path, s3_key)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.skipif(is_windows, reason="POSIX path rules")
@pytest.mark.parametrize("s3_key", ["C:/a.txt", "a\\b.txt", "a:b/c.txt"])
def test_s3_key_to_local_path_allows_on_posix(tmp_path, s3_key):
    # on POSIX a backslash and a colon are ordinary characters in a file name
    assert s3_key_to_local_path(tmp_path, s3_key) == tmp_path.joinpath(*s3_key.split("/"))


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_download_to_directory_stays_in_directory(s3_access):
    # a key with a leading slash and a doubled slash is downloaded under the destination directory, for both download() and download_cached()
    s3_key = "/key_to_local_path//leading_slash.txt"
    s3_access.write_string("stays in the directory", s3_key)
    for sub_dir, download in (("download", s3_access.download), ("download_cached", s3_access.download_cached)):
        dest_dir = Path(temp_dir, "key_to_local_path", sub_dir)
        dest_dir.mkdir(parents=True, exist_ok=True)
        expected_path = Path(dest_dir, "key_to_local_path", "leading_slash.txt")
        expected_path.unlink(missing_ok=True)
        download(s3_key, dest_dir)
        assert expected_path.read_text() == "stays in the directory"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_download_to_directory_rejects_parent_directory_key(s3_access):
    # S3 itself does not take a key that goes above the bucket in the request path (e.g. "a/../../b" is a 400 Bad Request), so this key has one ".." (keys that go
    # above the destination directory are in test_s3_key_to_local_path_rejects, which needs no S3)
    s3_key = "key_to_local_path/../escaped.txt"
    s3_access.write_string("must not escape", s3_key)
    dest_dir = Path(temp_dir, "key_to_local_path", "reject")
    dest_dir.mkdir(parents=True, exist_ok=True)
    with pytest.raises(AWSimpleException):
        s3_access.download(s3_key, dest_dir)
    with pytest.raises(AWSimpleException):
        s3_access.download_cached(s3_key, dest_dir)
    for directory in (dest_dir, dest_dir.parent, dest_dir.parent.parent):
        assert not Path(directory, "escaped.txt").exists()
    assert s3_access.download(s3_key, Path(dest_dir, "named_by_the_caller.txt"))  # a full destination path is the choice of the caller, so the key is not part of it
