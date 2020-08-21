import time
from datetime import timedelta
from pathlib import Path
from math import isclose
import os
from shutil import rmtree

from awsimple import S3Access
from test_awsimple import test_awsimple_str

temp_dir = Path("temp")
temp_dir.mkdir(parents=True, exist_ok=True)
cache_dir = Path(temp_dir, "cache")
cache_dir.mkdir(parents=True, exist_ok=True)

never_change_file_name = "never_change.txt"
never_change_mtime = 1598046722.0


def test_s3():

    test_string = str(time.time())  # so it changes between tests

    s3_access = S3Access(profile_name=test_awsimple_str, bucket=test_awsimple_str)
    assert s3_access.create_bucket()
    s3_access.write_string(test_string, test_awsimple_str)
    assert s3_access.read_string(test_awsimple_str) == test_string


def test_aws_metadata():
    s3_access = S3Access(profile_name=test_awsimple_str, bucket=test_awsimple_str)
    size, mtime, s3_hash = s3_access.get_size_mtime_hash(never_change_file_name)
    mtime_epoch = mtime.timestamp()
    assert isclose(mtime_epoch, never_change_mtime, rel_tol=0.0, abs_tol=3.0)  # SWAG
    assert s3_hash == "0b344cb999fb3d07bffc558c0cdf33d5"
    assert size == 65


def test_s3_download():
    dest_path = Path(temp_dir, never_change_file_name)
    dest_path.unlink(missing_ok=True)
    s3_access = S3Access(profile_name=test_awsimple_str, bucket=test_awsimple_str)
    s3_access.download(dest_path, never_change_file_name)
    assert dest_path.exists()
    assert isclose(os.path.getmtime(dest_path), never_change_mtime, rel_tol=0.0, abs_tol=3.0)


def test_s3_download_cached():
    dest_path = Path(temp_dir, never_change_file_name)
    s3_access = S3Access(profile_name=test_awsimple_str, bucket=test_awsimple_str)

    # start with empty cache
    rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    s3_access.download_cached(dest_path, never_change_file_name, cache_dir=cache_dir)
    assert dest_path.exists()
    dest_path.unlink(missing_ok=True)

    # with warm cache
    s3_access.download_cached(dest_path, never_change_file_name, cache_dir=cache_dir)
    assert dest_path.exists()
    dest_path.unlink(missing_ok=True)


def test_aws_big_file():
    # make a big file

    temp_dir.mkdir(parents=True, exist_ok=True)

    big_file_name = "big.txt"
    big_last_run_file_path = Path(temp_dir, "big_last_run.txt")

    try:
        last_run = float(big_last_run_file_path.open().read().strip())
    except FileNotFoundError:
        last_run = 0.0

    # only run once a day max since it takes so long
    if last_run + timedelta(days=7).total_seconds() < time.time():

        big_file_path = Path(temp_dir, big_file_name)
        max_size = round(100E6)  # should be large enough to do a multi-part upload and would timeout with default AWS timeouts (we use longer timeouts than the defaults)
        size = max_size/1000  # start with something small
        while size < max_size:
            size *= 2  # get bigger on each iteration
            size = min(max_size, size)  # make sure at the end we do one of max size
            with big_file_path.open("w") as f:
                f.truncate(round(size))  # this quickly makes a (sparse) file filled with zeros
            start = time.time()
            s3_access = S3Access(profile_name=test_awsimple_str, bucket=test_awsimple_str)
            s3_access.upload(big_file_path, big_file_name)
            print(f"{time.time() - start},{size:.0f}")

        big_last_run_file_path.open('w').write(str(time.time()))
    else:
        print(f"last run {time.time() - last_run} seconds ago so not running now")
