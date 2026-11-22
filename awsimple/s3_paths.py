"""
S3 keys as local file paths.

An S3 key is any string, and a local file path is not: a key can start with "/" or have "//" or ".." in it, differ from another key only in case, or have characters the
OS does not allow in a file name. s3_key_to_local_path() is the one place a key becomes a local path. Never join a directory and a raw key.
"""

import os
import sys
from pathlib import Path

from typeguard import typechecked

from .exceptions import AWSimpleException

# True if file names that differ only in case are taken as the same file. This is the default of the OS (Windows and macOS), not a test of the file system a directory is on.
case_insensitive_file_names = sys.platform in ("win32", "darwin")


@typechecked()
def s3_key_to_local_path(dest_dir: Path, s3_key: str) -> Path:
    """
    Map an S3 key to a local file path that is under a destination directory: the non-empty "/" separated components of the key, joined under dest_dir. Empty components
    are dropped, as "aws s3 sync" does, so the key "/temp//a.txt" is temp/a.txt under dest_dir (a leading "/" does not go to the root of the drive or file system).

    Raises AWSimpleException if the key can not be a file under dest_dir with the names the key has: no components at all, a "." or ".." component, or a component that the
    OS does not take as one file name (on Windows: a backslash, which is a path separator, or a colon, which is a drive or an alternate data stream).

    :param dest_dir: destination directory
    :param s3_key: S3 key
    :return: local file path under dest_dir
    """
    components = [component for component in s3_key.split("/") if len(component) > 0]
    local_path = dest_dir.joinpath(*components)
    is_single_names = local_path.parts == dest_dir.parts + tuple(components)  # False if the OS took a component as a drive, a root, more than one name, or nothing
    has_colon_on_windows = os.name == "nt" and any(":" in component for component in components)
    if len(components) == 0 or "." in components or ".." in components or not is_single_names or has_colon_on_windows:
        raise AWSimpleException(f'S3 key "{s3_key}" can not be a file under "{dest_dir}"')
    return local_path


@typechecked()
def local_path_collision_name(relative_path: Path) -> str:
    """
    Get a string that is the same for local paths that are the same file, so that S3 keys that would be the same local file can be found before anything is written.

    :param relative_path: a local path, relative to the directory the keys are mapped under
    :return: a string that is the same for paths that are the same local file
    """
    return str(relative_path).casefold() if case_insensitive_file_names else str(relative_path)
