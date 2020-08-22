from pathlib import Path
from shutil import disk_usage, copy2
import os

from typeguard import typechecked
from balsa import get_logger

from awsimple import __application_name__

log = get_logger(__application_name__)


@typechecked(always=True)
def get_disk_free(path: Path = Path(".")) -> int:
    total, used, free = disk_usage(Path(path).absolute().anchor)
    log.info(f"{total=} {used=} {free=}")
    return free


@typechecked(always=True)
def get_directory_size(path: Path) -> int:
    size = 0
    for p in path.glob("*"):
        if p.is_file():
            size += os.path.getsize(p)
        elif p.is_dir():
            size += get_directory_size(p)
    return size


@typechecked(always=True)
def lru_cache_write(new_file: Path, cache_dir: Path, max_absolute_cache_size: int = None, max_free_portion: float = None) -> bool:
    """
    free up space in the LRU cache to make room for the new file
    :param new_file: path to new file we want to put in the cache
    :param cache_dir: cache directory
    :param max_absolute_cache_size: max absolute cache size (or None if not specified)
    :param max_free_portion: max portion of disk free space the cache is allowed to consume (e.g. 0.1 to take up to 10% of free disk space)
    :return: True if there is now enough space for the new file
    """

    least_recently_used_path = None
    least_recently_used_access_time = None
    least_recently_used_size = None
    success = False

    try:
        max_free_absolute = max_free_portion * get_disk_free() if max_free_portion is not None else None
        values = [v for v in [max_free_absolute, max_absolute_cache_size] if v is not None]
        max_cache_size = min(values) if len(values) > 0 else None
        log.info(f"{max_cache_size=}")

        if max_cache_size is None:
            success = True  # no max cache size given - we don't have to free up anything so we don't have to do anything to be "successful"
        else:

            overage = (get_directory_size(cache_dir) + os.path.getsize(new_file)) - max_cache_size

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
                    least_recently_used_path.unlink()
                    overage -= least_recently_used_size

                if overage == starting_overage:
                    # tried to free up space but were unsuccessful, so give up
                    overage = 0

            success = get_directory_size(cache_dir) <= max_cache_size

    except (FileNotFoundError, IOError, PermissionError) as e:
        log.warning(f"{least_recently_used_path=} {least_recently_used_access_time=} {least_recently_used_size=} {e}")

    if success:
        copy2(new_file, cache_dir)

    return success
