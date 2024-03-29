import time
from datetime import timedelta
from pathlib import Path
from math import isclose
import os
from shutil import rmtree
from logging import getLogger

from awsimple import S3Access, get_directory_size, is_mock, is_using_localstack
from test_awsimple import test_awsimple_str, never_change_file_name, temp_dir, cache_dir

big_file_name = "big.txt"
big_file_max_size = round(100e6)  # should be large enough to do a multipart upload and would time out with default AWS timeouts (we use longer timeouts than the defaults)

# real AWS
never_change_size = 67
never_change_mtime = 1636830116.0
never_change_etag = "e3cb2ac8d7d4a8339ea3653f4f155ab4"

log = getLogger(__name__)


def test_get_never_change_metadata(s3_access) -> (int, float, str):
    global never_change_size, never_change_mtime, never_change_etag

    if is_mock() or is_using_localstack():
        # mocking always starts with nothing so we need up "upload" this file, but use boto3 so we don't write awsimple's SHA512.
        # localstack is similar in that we need to ensure we make the file.

        test_file_path = Path(temp_dir, never_change_file_name)
        never_change_file_contents = "modification Aug 21, 2020 at 2:51 PM PT\nnever change this file\n"
        test_file_path.open("w").write(never_change_file_contents)
        s3_access.client.upload_file(str(test_file_path), test_awsimple_str, never_change_file_name)  # no awsimple SHA512

        keys = [obj["Key"] for obj in s3_access.client.list_objects_v2(Bucket=test_awsimple_str)["Contents"]]
        assert never_change_file_name in keys

        metadata = s3_access.get_s3_object_metadata(never_change_file_name)
        never_change_mtime = metadata.mtime.timestamp()
        never_change_etag = metadata.etag
        never_change_size = metadata.size


def test_s3_read_string(s3_access):
    test_string = str(time.time())  # so it changes between tests

    # s3_access.create_bucket()  # may already exist
    s3_access.write_string(test_string, test_awsimple_str)
    assert s3_access.read_string(test_awsimple_str) == test_string


def test_s3_big_file_upload(s3_access):
    # test big file upload (e.g. that we don't get a timeout)
    # this is run before the cache tests (hence the function name)

    big_last_run_file_path = Path("big_last_run.txt")
    big_last_run_file_path.parent.mkdir(exist_ok=True, parents=True)
    last_run = 0.0
    if not (is_mock() or is_using_localstack()):
        # avoid large frequent file uploads with real AWS
        try:
            last_run = float(big_last_run_file_path.open().read().strip())
        except FileNotFoundError:
            pass

    # only run once a day max since it takes so long
    if last_run + timedelta(days=1).total_seconds() < time.time():
        big_file_path = Path(temp_dir, big_file_name)
        size = big_file_max_size / 1000  # start with something small
        while size < big_file_max_size:
            size *= 2  # get bigger on each iteration
            size = min(big_file_max_size, size)  # make sure at the end we do one of max size
            with big_file_path.open("w") as f:
                f.truncate(round(size))  # this quickly makes a (sparse) file filled with zeros
            start = time.time()
            s3_access.upload(big_file_path, big_file_name)
            log.info(f"{time.time() - start},{size:.0f}")

        big_last_run_file_path.open("w").write(str(time.time()))
    else:
        log.info(f"last run {time.time() - last_run} seconds ago so not running now")


def test_s3_upload(s3_access):
    test_file_name = "test.txt"
    test_file_path = Path(temp_dir, test_file_name)
    test_file_path.open("w").write("hello world")
    assert s3_access.upload(test_file_path, test_file_name, force=True)
    time.sleep(3)
    assert s3_access.object_exists(test_file_name)


def test_s3_z_metadata(s3_access):
    # does not work for mock todo: fix
    test_file_name = "test.txt"
    s3_object_metadata = s3_access.get_s3_object_metadata(test_file_name)
    # "hello world" uploaded with awsimple
    assert s3_object_metadata.sha512 == "309ecc489c12d6eb4cc40f50c902f2b4d0ed77ee511a7c7a9bcd3ca86d4cd86f989dd35bc5ff499670da34255b45b0cfd830e81f605dcf7dc5542e93ae9cd76f"
    assert s3_object_metadata.size == 11


def test_s3_download_dest_full_path(s3_access):
    dest_path = Path(temp_dir, never_change_file_name)
    dest_path.unlink(missing_ok=True)
    success = s3_access.download(never_change_file_name, dest_path)  # dest is a full path
    assert success
    assert dest_path.exists()
    assert isclose(os.path.getmtime(dest_path), never_change_mtime, rel_tol=0.0, abs_tol=3.0)


def test_s3_download_dest_dir(s3_access):
    dest_path = Path(temp_dir, never_change_file_name)
    dest_path.unlink(missing_ok=True)
    success = s3_access.download(never_change_file_name, temp_dir)  # dest is a directory
    assert success
    assert dest_path.exists()
    assert isclose(os.path.getmtime(dest_path), never_change_mtime, rel_tol=0.0, abs_tol=3.0)


def test_s3_metadata_not_uploaded_with_awsimple(s3_access):
    bucket_dir = s3_access.dir()
    assert len(bucket_dir) > 0
    assert bucket_dir["never_change.txt"].size == never_change_size
    s3_object_metadata = s3_access.get_s3_object_metadata(never_change_file_name)
    mtime_epoch = s3_object_metadata.mtime.timestamp()
    assert isclose(mtime_epoch, never_change_mtime, rel_tol=0.0, abs_tol=3.0)  # SWAG
    assert s3_object_metadata.etag == never_change_etag
    assert s3_object_metadata.sha512 is None  # not uploaded with awsimple
    assert s3_object_metadata.size == never_change_size


def _s3_download(dest: Path, s3_access):
    """
    :param dest: directory or file path to download to
    :param s3_access: S3Access
    """
    dest_path = Path(temp_dir, never_change_file_name)  # expect file to be downloaded here
    # start with empty cache
    rmtree(cache_dir, ignore_errors=True)
    cache_dir.mkdir(parents=True, exist_ok=True)
    dest_path.unlink(missing_ok=True)
    download_status = s3_access.download_cached(never_change_file_name, dest)
    assert download_status.success
    assert not download_status.cache_hit
    assert download_status.cache_write
    assert dest_path.exists()
    # download cached
    dest_path.unlink()
    download_status = s3_access.download_cached(never_change_file_name, dest)
    assert download_status.success
    assert download_status.cache_hit
    assert not download_status.cache_write
    assert dest_path.exists()

    # with warm cache
    dest_path.unlink()
    download_status = s3_access.download_cached(never_change_file_name, dest)
    assert download_status.success
    assert download_status.cache_hit
    assert dest_path.exists()


def _s3_download_big(dest: Path, s3_access):
    # download big file with normal cache size
    cache_size = get_directory_size(cache_dir)
    assert cache_size < 1000  # big file not in cache
    big_file_path = Path(temp_dir, big_file_name)
    download_status = s3_access.download_cached(big_file_name, dest)
    assert download_status.success
    assert not download_status.cache_hit
    assert download_status.cache_write
    assert big_file_path.exists()
    cache_size = get_directory_size(cache_dir)
    assert cache_size > 1000  # big file is in cache


def test_s3_download_cached(s3_access):
    _s3_download(Path(temp_dir, never_change_file_name), s3_access)  # small file with no AWSimple SHA512
    _s3_download_big(Path(temp_dir, big_file_name), s3_access)


def test_s3_download_cached_dir(s3_access):
    _s3_download(temp_dir, s3_access)
    _s3_download_big(temp_dir, s3_access)


def test_cache_eviction(s3_access):
    # force cache eviction
    cache_max = 100
    eviction_dir = Path(temp_dir, "eviction")
    eviction_cache = Path(eviction_dir, "cache")
    s3_access_cache_eviction = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str, cache_dir=eviction_cache, cache_max_absolute=cache_max)
    size = 50
    rmtree(eviction_dir, ignore_errors=True)
    while size <= 2 * cache_max:
        file_name = f"t{size}.txt"
        source_file_path = Path(eviction_dir, "source", file_name)
        source_file_path.parent.mkdir(parents=True, exist_ok=True)

        # upload
        with source_file_path.open("w") as f:
            f.truncate(round(size))  # this quickly makes a (sparse) file filled with zeros
        s3_access_cache_eviction.upload(source_file_path, file_name)

        dest_path = Path(eviction_dir, "dest", file_name)

        # cold download
        status_cold = s3_access_cache_eviction.download_cached(file_name, dest_path)
        assert not status_cold.cache_hit
        if size <= cache_max:
            assert status_cold.cache_write

        # warm download
        assert dest_path.exists()
        status_warm = s3_access_cache_eviction.download_cached(file_name, dest_path)
        if size <= cache_max:
            assert status_warm.cache_hit
            assert not status_warm.cache_write
        assert dest_path.exists()

        # make sure cache stays within max size limit
        cache_size = get_directory_size(eviction_cache)
        assert cache_size <= cache_max  # make sure we stay within bounds

        size *= 2
