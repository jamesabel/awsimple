"""
awsimple on free-threaded Python (the builds without the GIL, e.g. python3.14t), and awsimple used from several threads at once (on any build).

All tests, fixtures, helpers and constants in this file are AI-generated (Claude Code).
"""

import hashlib
import sys
import sysconfig
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier

import pytest

import awsimple
from awsimple import AWSAccess, S3Access, lru_cache_write
from test_awsimple import test_awsimple_str

is_free_threaded_build = bool(sysconfig.get_config_var("Py_GIL_DISABLED"))
thread_count = 8


def _in_threads(function, arguments: list) -> list:
    """
    Call a function in one thread per argument, all starting at the same time, and return the results (an exception in a thread is raised here).
    """
    barrier = Barrier(len(arguments))

    def _call(argument):
        barrier.wait(60.0)
        return function(argument)

    with ThreadPoolExecutor(len(arguments)) as executor:
        return list(executor.map(_call, arguments))


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.skipif(not is_free_threaded_build, reason="a free-threaded Python build")
def test_gil_stays_disabled():
    # An extension module that does not support free threading makes Python turn the GIL back on when it is imported. awsimple and everything it imports
    # (and everything the tests have imported by now, e.g. boto3, awscrt, moto and Pillow) must not do that.
    assert awsimple.__name__ in sys.modules
    assert not sys._is_gil_enabled()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_aws_access_from_threads():
    # the moto mock and its saved environment are shared by every AWSAccess in the process: making and releasing them from threads at once leaves both as they were
    def _make_and_release(_unused_number: int) -> bool:
        for _unused_pass in range(20):
            aws_access = AWSAccess()
            is_mocked = aws_access.is_mocked()
            del aws_access
        return is_mocked

    is_mocked_before = AWSAccess().is_mocked()
    assert _in_threads(_make_and_release, list(range(thread_count))) == [is_mocked_before] * thread_count
    assert AWSAccess().is_mocked() == is_mocked_before
    assert S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str).bucket_exists()  # the mock (and what is in it) is still there


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_cached_downloads_from_threads(s3_access, tmp_path):
    # threads, each with an S3Access of its own, download the same S3 objects through one cache directory at once: each gets the right contents
    contents = {f"free_threading/object_{number}.txt": f"contents of object {number} ".encode() * 1000 for number in range(4)}
    for s3_key, content in contents.items():
        s3_access.write_string(content.decode(), s3_key)
    cache_dir = Path(tmp_path, "cache")

    def _download_all(thread_number: int) -> int:
        thread_s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str, cache_dir=cache_dir)
        hits = 0
        for _unused_pass in range(3):
            for s3_key, content in contents.items():
                dest_path = Path(tmp_path, f"dest_{thread_number}", Path(s3_key).name)
                status = thread_s3_access.download_cached(s3_key, dest_path)
                assert status.success
                assert dest_path.read_bytes() == content
                hits += 1 if status.cache_hit else 0
        return hits

    hits = _in_threads(_download_all, list(range(thread_count)))
    assert all(thread_hits >= 2 * len(contents) for thread_hits in hits)  # at least every download after a thread's first pass
    cache_files = sorted(path.name for path in cache_dir.iterdir() if path.is_file())
    assert cache_files == sorted(hashlib.sha512(content).hexdigest() for content in contents.values())  # no temporary file is left, and nothing else
    for content in contents.values():
        assert Path(cache_dir, hashlib.sha512(content).hexdigest()).read_bytes() == content


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_lru_cache_writes_from_threads(tmp_path):
    # threads write to a cache that is too small for everything, so each write also evicts: every write either is in the cache whole or is not there at all
    cache_dir = Path(tmp_path, "cache")
    entry_size = 1000
    max_size = 5 * entry_size

    def _write(thread_number: int) -> int:
        written = 0
        for entry_number in range(10):
            written += 1 if lru_cache_write(bytes([thread_number]) * entry_size, cache_dir, f"entry_{thread_number}_{entry_number}", max_absolute_cache_size=max_size) else 0
        return written

    written = _in_threads(_write, list(range(thread_count)))
    assert sum(written) > 0
    for cache_file in cache_dir.iterdir():
        thread_number = int(cache_file.name.split("_")[1])
        assert cache_file.read_bytes() == bytes([thread_number]) * entry_size


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.skipif(not awsimple.is_mock(), reason="the moto mock")
def test_moto_s3_does_not_reload_its_urls(s3_access):
    # the moto mock of S3 reloads a module on every request unless awsimple stops it (see _make_moto_s3_safe_for_threads()), which fails from threads
    import moto.s3.urls as moto_s3_urls
    from moto.s3.models import S3Backend

    assert s3_access.is_mocked()
    assert S3Backend._url_module.fget(None) is moto_s3_urls
    s3_access.write_string("threads", "free_threading/reload.txt")
    assert _in_threads(lambda _unused_number: [s3_access.read_string("free_threading/reload.txt") for _unused_pass in range(25)][-1], list(range(thread_count))) == ["threads"] * thread_count
