from pathlib import Path
from shutil import disk_usage

from balsa import get_logger

from awsimple import __application_name__

log = get_logger(__application_name__)


def get_disk_free() -> int:
    total, used, free = disk_usage(Path(".").absolute().anchor)
    log.info(f"{total=} {used=} {free=}")
    return free


def remove_lru(cache_dir: Path, max_cache_size: int):
    pass
