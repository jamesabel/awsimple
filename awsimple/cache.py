from pathlib import Path
from shutil import disk_usage
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
def remove_lru(cache_dir: Path, max_cache_size: int):
    raise NotImplementedError  # todo: finish this
