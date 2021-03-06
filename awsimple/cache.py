from pathlib import Path
from shutil import disk_usage, copy2
import os
from logging import getLogger
import math

from typeguard import typechecked
from appdirs import user_cache_dir

from awsimple import __application_name__, __author__, AWSAccess

log = getLogger(__application_name__)


@typechecked()
def get_disk_free(path: Path = Path(".")) -> int:
    total, used, free = disk_usage(Path(path).absolute().anchor)
    log.info(f"{total=} {used=} {free=}")
    return free


@typechecked()
def get_directory_size(path: Path) -> int:
    size = 0
    for p in path.glob("*"):
        if p.is_file():
            size += os.path.getsize(p)
        elif p.is_dir():
            size += get_directory_size(p)
    return size


@typechecked()
def lru_cache_write(new_file: Path, cache_dir: Path, cache_file_name: str, max_absolute_cache_size: int = None, max_free_portion: float = None) -> bool:
    """
    free up space in the LRU cache to make room for the new file
    :param new_file: path to new file we want to put in the cache
    :param cache_dir: cache directory
    :param cache_file_name: file name to write in cache
    :param max_absolute_cache_size: max absolute cache size (or None if not specified)
    :param max_free_portion: max portion of disk free space the cache is allowed to consume (e.g. 0.1 to take up to 10% of free disk space)
    :return: True wrote to cache
    """

    least_recently_used_path = None
    least_recently_used_access_time = None
    least_recently_used_size = None
    wrote_to_cache = False

    try:
        max_free_absolute = max_free_portion * get_disk_free() if max_free_portion is not None else None
        values = [v for v in [max_free_absolute, max_absolute_cache_size] if v is not None]
        max_cache_size = min(values) if len(values) > 0 else None
        log.info(f"{max_cache_size=}")

        new_file_size = os.path.getsize(new_file)

        if max_cache_size is None:
            is_room = True  # no limit
        elif new_file_size > max_cache_size:
            log.info(f"{new_file=} {new_file_size=} is larger than the cache itself {max_cache_size=}")
            is_room = False  # new file will never fit so don't try to evict to make room for it
        else:

            cache_size = get_directory_size(cache_dir)
            overage = (cache_size + new_file_size) - max_cache_size

            # cache eviction
            while overage > 0:
                starting_overage = overage

                # find the least recently used file
                least_recently_used_path = None
                least_recently_used_access_time = None
                least_recently_used_size = None
                for file_path in cache_dir.rglob("*"):
                    access_time = os.path.getatime(file_path)
                    if least_recently_used_path is None or access_time < least_recently_used_access_time:
                        least_recently_used_path = file_path
                        least_recently_used_access_time = access_time
                        least_recently_used_size = os.path.getsize(file_path)

                if least_recently_used_path is not None:
                    log.debug(f"evicting {least_recently_used_path=} {least_recently_used_access_time=} {least_recently_used_size=}")
                    least_recently_used_path.unlink()
                    overage -= least_recently_used_size

                if overage == starting_overage:
                    # tried to free up space but were unsuccessful, so give up
                    overage = 0

            # determine if we have room for the new file
            is_room = get_directory_size(cache_dir) + new_file_size <= max_cache_size

        if is_room:
            cache_dest = Path(cache_dir, cache_file_name)
            log.info(f"caching {new_file=} to {cache_dest=}")
            copy2(new_file, cache_dest)
            wrote_to_cache = True
        else:
            log.info(f"no room for {new_file=}")

    except (FileNotFoundError, IOError, PermissionError) as e:
        log.warning(f"{least_recently_used_path=} {least_recently_used_access_time=} {least_recently_used_size=} {e}")

    return wrote_to_cache


class CacheAccess(AWSAccess):
    def __init__(
        self,
        resource_name: str = None,
        cache_dir: Path = None,
        cache_life: float = math.inf,
        cache_max_absolute: int = round(1e9),
        cache_max_of_free: float = 0.05,
        mtime_abs_tol: float = 10.0,
        **kwargs,
    ):
        """
        AWS Access for cacheables

        :param cache_dir: dir for cache
        :param cache_life: life of cache (in seconds)
        :param cache_max_absolute: max size of cache
        :param cache_max_of_free: max portion of disk free space the cache will consume
        :param mtime_abs_tol: window in seconds where a modification time will be considered equal
        """

        if cache_dir is None:
            if resource_name is None:
                self.cache_dir = Path(user_cache_dir(__application_name__, __author__), "aws")
            else:
                self.cache_dir = Path(user_cache_dir(__application_name__, __author__), "aws", resource_name)
        else:
            self.cache_dir = cache_dir

        self.cache_life = cache_life  # seconds
        self.cache_max_absolute = cache_max_absolute  # max absolute cache size
        self.cache_max_of_free = cache_max_of_free  # max portion of the disk's free space this LRU cache will take
        self.cache_retries = 10  # cache upload retries
        self.mtime_abs_tol = mtime_abs_tol  # file modification times within this cache window (in seconds) are considered equivalent

        super().__init__(resource_name, **kwargs)
