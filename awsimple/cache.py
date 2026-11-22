"""
Local caching: the cache directory and its limits (CacheAccess), a least-recently-used (LRU) file cache with a maximum size, and atomic file writes.

Every write to a cache goes through atomic_write_path(), so a cache file is never seen partly written.
"""

from pathlib import Path
from shutil import disk_usage, copy2
import os
import math
import time
import uuid
from contextlib import contextmanager
from typing import Generator, Union
from logging import getLogger

from typeguard import typechecked
from appdirs import user_cache_dir

from awsimple import __application_name__, __author__, AWSAccess

log = getLogger(__application_name__)

CACHE_DIR_ENV_VAR = f"{__application_name__}_CACHE_DIR".upper()

atomic_write_temp_suffix = ".tmp"  # suffix of the temporary file of an atomic write (see atomic_write_path())


@typechecked()
def get_disk_free(path: Path = Path(".")) -> int:
    """
    Get the free space of the disk a path is on (the disk of the path's drive or root).

    :param path: a path on the disk
    :return: free space in bytes
    """
    total, used, free = disk_usage(Path(path).absolute().anchor)
    log.info(f"{total=} {used=} {free=}")
    return free


@typechecked()
def get_directory_size(path: Path) -> int:
    """
    Get the total size of the files in a directory, including its sub-directories.

    :param path: directory path
    :return: size in bytes
    """
    size = 0
    for p in path.glob("*"):
        if p.is_file():
            size += os.path.getsize(p)
        elif p.is_dir():
            size += get_directory_size(p)
    return size


@contextmanager
def atomic_write_path(dest_path: Path) -> Generator[Path, None, None]:
    """
    Make a write to a file atomic: write to the temporary path this yields, and when the block ends without an exception the temporary file is moved to dest_path in one step.
    dest_path is therefore either absent, its previous contents, or all of its new contents - never part of them. The temporary file is in the same directory as dest_path
    (so the move is within one file system) and is always removed. If the process is killed during the write, the temporary file that is left behind is never used as
    dest_path (and in an LRU cache directory it is evicted like any other file).

    :param dest_path: path of the file to write
    :return: yields the temporary path to write to
    """
    temp_path = dest_path.with_name(f"{dest_path.name}.{uuid.uuid4().hex}{atomic_write_temp_suffix}")
    try:
        yield temp_path
        os.replace(temp_path, dest_path)
    finally:
        temp_path.unlink(missing_ok=True)


def _evict_least_recently_used(cache_dir: Path) -> Union[int, None]:
    """
    Delete the least recently used file in the cache.

    :param cache_dir: cache directory
    :return: size of the deleted file in bytes (which can be 0), or None if there was no file to delete
    """
    least_recently_used_path = None
    least_recently_used_access_time = None
    for file_path in cache_dir.rglob("*"):
        if not file_path.is_file():
            continue  # only files are evicted (rglob can also return directories)
        access_time = os.path.getatime(file_path)
        if least_recently_used_access_time is None or access_time < least_recently_used_access_time:
            least_recently_used_path = file_path
            least_recently_used_access_time = access_time

    if least_recently_used_path is None:
        return None
    size = os.path.getsize(least_recently_used_path)
    log.debug(f"evicting {least_recently_used_path=} {least_recently_used_access_time=} {size=}")
    least_recently_used_path.unlink()
    return size


def _make_room(cache_dir: Path, new_size: int, max_cache_size: int) -> bool:
    """
    Evict the least recently used files until the new data fits in the cache.

    :param cache_dir: cache directory
    :param new_size: size of the new data in bytes
    :param max_cache_size: the most the cache may hold in bytes
    :return: True if there is room for the new data
    """
    if new_size > max_cache_size:
        log.info(f"{new_size=} is larger than the cache itself {max_cache_size=}")
        return False  # the new data will never fit, so don't evict to make room for it

    overage = (get_directory_size(cache_dir) + new_size) - max_cache_size
    while overage > 0:
        evicted_size = _evict_least_recently_used(cache_dir)
        if evicted_size is None:
            break  # nothing is left to evict, so give up
        overage -= evicted_size

    return get_directory_size(cache_dir) + new_size <= max_cache_size


@typechecked()
def lru_cache_mark_used(cache_file_path: Path):
    """
    Mark a file in an LRU cache as used now, by setting its access time (its modification time is kept). The cache evicts the least recently accessed file, and the OS
    does not reliably update the access time when a file is read (e.g. Windows by default, and Linux file systems mounted with relatime or noatime), so every use of a
    cache file calls this.

    :param cache_file_path: path to a file in the cache
    """
    try:
        os.utime(cache_file_path, (time.time(), os.path.getmtime(cache_file_path)))
    except OSError as e:
        # e.g. another process evicted the file, or the cache is read-only. The file is then only evicted sooner than it would have been.
        log.info(f"{cache_file_path=} {e}")


@typechecked()
def lru_cache_write(
    new_data: Union[Path, bytes], cache_dir: Path, cache_file_name: str, max_absolute_cache_size: Union[int, None] = None, max_free_portion: Union[float, None] = None, extra_size: int = 0
) -> bool:
    """
    Write a file to an LRU cache: free up space by evicting the least recently used files until the new file fits, then write it atomically (see atomic_write_path())
    and mark it as used (see lru_cache_mark_used()). Not being able to write to the cache is a normal outcome (e.g. the new file is larger than the cache), not an error.

    :param new_data: path to new file or a bytes object we want to put in the cache
    :param cache_dir: cache directory
    :param cache_file_name: file name to write in cache
    :param max_absolute_cache_size: max absolute cache size (or None if not specified)
    :param max_free_portion: max portion of disk free space the cache is allowed to consume (e.g. 0.1 to take up to 10% of free disk space)
    :param extra_size: room in bytes to also make in the cache, for a file the caller writes to the cache directory itself to go with this one (so both are within the maximum)
    :return: True if written to the cache
    """

    wrote_to_cache = False

    try:
        max_free_absolute = max_free_portion * get_disk_free(cache_dir) if max_free_portion is not None else None
        values = [v for v in [max_free_absolute, max_absolute_cache_size] if v is not None]
        max_cache_size = min(values) if len(values) > 0 else None
        log.info(f"{max_cache_size=}")

        new_size = os.path.getsize(new_data) if isinstance(new_data, Path) else len(new_data)

        # no maximum: there is always room
        if max_cache_size is None or _make_room(cache_dir, new_size + extra_size, round(max_cache_size)):
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_dest = Path(cache_dir, cache_file_name)
            with atomic_write_path(cache_dest) as temp_path:
                if isinstance(new_data, Path):
                    log.info(f"caching {new_data} to {cache_dest=}")
                    copy2(new_data, temp_path)
                else:
                    log.info(f"caching {len(new_data)}B to {cache_dest=}")
                    temp_path.write_bytes(new_data)
            lru_cache_mark_used(cache_dest)  # a file copied into the cache can have the access time of the file it was copied from
            wrote_to_cache = True
        else:
            log.info(f"no room for {new_data=}")

    except (FileNotFoundError, IOError, PermissionError) as e:
        log.debug(f"{cache_dir=} {cache_file_name=} {e}", stack_info=True, exc_info=True)

    return wrote_to_cache


class CacheAccess(AWSAccess):
    """
    AWS access for resources that awsimple caches locally (S3 objects and DynamoDB table scans): the cache directory and its limits.
    """

    def __init__(
        self,
        resource_name: str,
        cache_dir: Union[Path, None] = None,
        cache_life: float = math.inf,
        cache_max_absolute: int = round(1e9),
        cache_max_of_free: float = 0.05,
        mtime_abs_tol: float = 10.0,
        use_env_var_cache_dir: bool = False,
        **kwargs,
    ):
        """
        AWS Access for cacheables

        :param cache_dir: dir for cache
        :param cache_life: life of cache (in seconds)
        :param cache_max_absolute: max size of cache
        :param cache_max_of_free: max portion of disk free space the cache will consume
        :param mtime_abs_tol: window in seconds where a modification time will be considered equal
        :param use_env_var_cache_dir: set to True to attempt to use environmental variable for the cache dir (user must explicitly set this to use env var for cache dir)
        """

        self.use_env_var_cache_dir = use_env_var_cache_dir
        if cache_dir is not None:
            self.cache_dir = cache_dir  # passing cache dir in takes precedent
        elif self.use_env_var_cache_dir and (cache_dir_from_env_var := os.environ.get(CACHE_DIR_ENV_VAR)) is not None:
            self.cache_dir = Path(cache_dir_from_env_var.strip())
        else:
            self.cache_dir = Path(user_cache_dir(__application_name__, __author__), "aws", resource_name)

        self.cache_life = cache_life  # seconds
        self.cache_max_absolute = cache_max_absolute  # max absolute cache size
        self.cache_max_of_free = cache_max_of_free  # max portion of the disk's free space this LRU cache will take
        self.cache_retries = 10  # cache upload retries
        self.mtime_abs_tol = mtime_abs_tol  # file modification times within this cache window (in seconds) are considered equivalent

        super().__init__(resource_name, **kwargs)
