import time
from datetime import timedelta
from pathlib import Path
from math import isclose
import os
from shutil import rmtree

from awsimple import S3Access, get_directory_size, is_mock
from test_awsimple import test_awsimple_str

temp_dir = Path("temp")
temp_dir.mkdir(parents=True, exist_ok=True)
cache_dir = Path(temp_dir, "cache")
cache_dir.mkdir(parents=True, exist_ok=True)

big_file_name = "big.txt"
big_file_max_size = round(100e6)  # should be large enough to do a multi-part upload and would timeout with default AWS timeouts (we use longer timeouts than the defaults)
never_change_file_name = "never_change.txt"
never_change_mtime = 1598046722.0


def test_s3_read_string():

    test_string = str(time.time())  # so it changes between tests

    s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)
    s3_access.create_bucket()  # may already exist
    s3_access.write_string(test_string, test_awsimple_str)
    assert s3_access.read_string(test_awsimple_str) == test_string


def test_s3_big_file_upload():
    # test big file upload (e.g. that we don't get a timeout)
    # this is run before the cache tests (hence the function name)

    temp_dir.mkdir(parents=True, exist_ok=True)

    big_last_run_file_path = Path(temp_dir, "big_last_run.txt")
    try:
        last_run = float(big_last_run_file_path.open().read().strip())
    except FileNotFoundError:
        last_run = 0.0

    # only run once a day max since it takes so long
    if last_run + timedelta(days=1).total_seconds() < time.time():

        s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)
        big_file_path = Path(temp_dir, big_file_name)
        size = big_file_max_size / 1000  # start with something small
        while size < big_file_max_size:
            size *= 2  # get bigger on each iteration
            size = min(big_file_max_size, size)  # make sure at the end we do one of max size
            with big_file_path.open("w") as f:
                f.truncate(round(size))  # this quickly makes a (sparse) file filled with zeros
            start = time.time()
            s3_access.upload(big_file_path, big_file_name)
            print(f"{time.time() - start},{size:.0f}")

        big_last_run_file_path.open("w").write(str(time.time()))
    else:
        print(f"last run {time.time() - last_run} seconds ago so not running now")


def test_s3_upload():
    s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)
    test_file_name = "test.txt"
    test_file_path = Path(temp_dir, test_file_name)
    test_file_path.open("w").write("hello world")
    assert s3_access.upload(test_file_path, test_file_name, force=True)
    time.sleep(3)
    assert s3_access.object_exists(test_file_name)


def test_s3_metadata_not_uploaded_with_awsimple():
    if not is_mock():
        # does not work for mock todo: fix
        s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)
        s3_object_metadata = s3_access.get_s3_object_metadata(never_change_file_name)
        mtime_epoch = s3_object_metadata.mtime.timestamp()
        assert isclose(mtime_epoch, never_change_mtime, rel_tol=0.0, abs_tol=3.0)  # SWAG
        assert s3_object_metadata.etag == "0b344cb999fb3d07bffc558c0cdf33d5"
        assert s3_object_metadata.sha512 is None  # not uploaded with awsimple
        assert s3_object_metadata.size == 65


def test_s3_z_metadata():
    if not is_mock():
        # does not work for mock todo: fix
        test_file_name = "test.txt"
        s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)
        s3_object_metadata = s3_access.get_s3_object_metadata(test_file_name)
        # "hello world" uploaded with awsimple
        assert s3_object_metadata.sha512 == "309ecc489c12d6eb4cc40f50c902f2b4d0ed77ee511a7c7a9bcd3ca86d4cd86f989dd35bc5ff499670da34255b45b0cfd830e81f605dcf7dc5542e93ae9cd76f"
        assert s3_object_metadata.size == 11


def test_s3_download():
    if not is_mock():
        # does not work for mock todo: fix
        dest_path = Path(temp_dir, never_change_file_name)
        dest_path.unlink(missing_ok=True)
        s3_access = S3Access(test_awsimple_str, profile_name=test_awsimple_str)
        s3_access.download(never_change_file_name, dest_path)
        assert dest_path.exists()
        assert isclose(os.path.getmtime(dest_path), never_change_mtime, rel_tol=0.0, abs_tol=3.0)


def test_s3_download_cached():
    # runs last

    if not is_mock():
        # does not work for mock todo: fix
        dest_path = Path(temp_dir, never_change_file_name)  # small file
        s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str, cache_dir=cache_dir)

        # start with empty cache
        rmtree(cache_dir, ignore_errors=True)
        cache_dir.mkdir(parents=True, exist_ok=True)
        dest_path.unlink(missing_ok=True)
        s3_access.download_cached(never_change_file_name, dest_path)
        assert dest_path.exists()

        # with warm cache
        dest_path.unlink()
        s3_access.download_cached(never_change_file_name, dest_path)
        assert dest_path.exists()

        # download big file with normal cache size
        cache_size = get_directory_size(cache_dir)
        print(f"{cache_size=}")
        assert cache_size < 1000  # big file not in cache
        big_file_path = Path(temp_dir, big_file_name)
        s3_access.download_cached(big_file_name, big_file_path)
        assert big_file_path.exists()
        cache_size = get_directory_size(cache_dir)
        print(f"{cache_size=}")
        assert cache_size > 1000  # big file is in cache


def test_cache_eviction():
    # force cache eviction
    cache_max = 100
    eviction_dir = Path(temp_dir, "eviction")
    eviction_cache = Path(eviction_dir, "cache")
    s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str, cache_dir=eviction_cache, cache_max_absolute=cache_max)
    size = 50
    rmtree(eviction_dir, ignore_errors=True)
    while size <= 2 * cache_max:
        file_name = f"t{size}.txt"
        source_file_path = Path(eviction_dir, "source", file_name)
        source_file_path.parent.mkdir(parents=True, exist_ok=True)

        # upload
        with source_file_path.open("w") as f:
            f.truncate(round(size))  # this quickly makes a (sparse) file filled with zeros
        s3_access.upload(source_file_path, file_name)

        dest_path = Path(eviction_dir, "dest", file_name)

        # cold download
        status_cold = s3_access.download_cached(file_name, dest_path)
        assert not status_cold.cache_hit
        if size <= cache_max:
            assert status_cold.cache_write

        # warm download
        assert dest_path.exists()
        status_warm = s3_access.download_cached(file_name, dest_path)
        if size <= cache_max:
            assert status_warm.cache_hit
            assert not status_warm.cache_write
        assert dest_path.exists()

        # make sure cache stays within max size limit
        cache_size = get_directory_size(eviction_cache)
        print(f"{cache_size=}")
        assert cache_size <= cache_max  # make sure we stay within bounds

        size *= 2
