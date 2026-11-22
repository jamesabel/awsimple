"""
The local cache of S3 object contents.

Cached contents are in a file named with the SHA-512 of the contents, so S3 objects with the same contents are one cache file, and a cache file can always be checked
against its own name. The SHA-512 of an S3 object comes from its metadata (see S3ObjectMetadata.get_content_sha512()). An S3 object that has no SHA-512 (one that was
not written by awsimple) is found through the index: a file, named with the object's identity hash (see S3ObjectMetadata.get_identity_sha512()), that holds the SHA-512
awsimple calculated when it downloaded the object. awsimple never updates an S3 object's metadata on a read.

The cache directory is a least-recently-used cache with a maximum size (see lru_cache_write() in cache.py).
"""

import os
import shutil
from logging import getLogger
from pathlib import Path
from typing import Union

from hashy import get_file_sha512

from .__version__ import __application_name__
from .cache import atomic_write_path, lru_cache_mark_used, lru_cache_write
from .exceptions import AWSimpleException
from .s3_object import S3ObjectMetadata, set_file_mtime, sha512_hex_regex

# Sub-directory of the S3 cache directory. For an S3 object that has no SHA-512, it holds the SHA-512 of the contents awsimple downloaded, in a file named with the object's identity hash.
cache_index_dir_name = "index"

log = getLogger(__application_name__)


class S3ContentCache:
    """
    The local cache of S3 object contents in one cache directory.

    A cache hit takes a cache file that has the length of the S3 object and the SHA-512 it is named with, and that is still there when it is read (another process may
    evict it at any time). Anything else is a cache miss, never an error: the caller then downloads the S3 object and writes it to the cache with write().
    """

    def __init__(self, cache_dir: Path, max_absolute_size: Union[int, None], max_free_portion: Union[float, None]):
        """
        :param cache_dir: cache directory (made when something is first written to the cache)
        :param max_absolute_size: the most the cache may hold in bytes (or None for no absolute maximum)
        :param max_free_portion: the most the cache may hold as a portion of the disk's free space (or None)
        """
        self.cache_dir = cache_dir
        self.max_absolute_size = max_absolute_size
        self.max_free_portion = max_free_portion

    def _get_index_path(self, s3_object_metadata: S3ObjectMetadata) -> Path:
        """
        :param s3_object_metadata: S3 object metadata
        :return: path to the index file of the S3 object
        """
        return Path(self.cache_dir, cache_index_dir_name, s3_object_metadata.get_identity_sha512())

    def _get_content_sha512(self, s3_object_metadata: S3ObjectMetadata) -> Union[str, None]:
        """
        Get the SHA-512 of an S3 object's contents: the S3 object's SHA-512 if it has one, otherwise the SHA-512 in the index.

        :param s3_object_metadata: S3 object metadata
        :return: SHA-512 (as hex string), or None if it is not known
        """
        if (content_sha512 := s3_object_metadata.get_content_sha512()) is not None:
            return content_sha512
        index_path = self._get_index_path(s3_object_metadata)
        try:
            content_sha512 = index_path.read_text().strip()
        except FileNotFoundError:
            return None
        lru_cache_mark_used(index_path)
        if sha512_hex_regex.fullmatch(content_sha512) is None:
            return None
        return content_sha512

    def find(self, s3_object_metadata: S3ObjectMetadata) -> Union[Path, None]:
        """
        Find an S3 object's contents in the cache, and mark them as used.

        :param s3_object_metadata: S3 object metadata
        :return: path to the cached contents, or None if they are not in the cache (or the cache file does not have the contents it is named for)
        """
        if (content_sha512 := self._get_content_sha512(s3_object_metadata)) is None:
            return None
        cache_path = Path(self.cache_dir, content_sha512)
        try:
            # the cache file is only used if it has the length of the S3 object and the SHA-512 it is named with
            is_intact = os.path.getsize(cache_path) == s3_object_metadata.size and get_file_sha512(cache_path) == content_sha512
        except OSError as e:
            # not in the cache (FileNotFoundError), or it can not be read (e.g. on Windows, another process is writing it)
            log.debug(f"{cache_path=} {e}")
            return None
        if not is_intact:
            log.warning(f'cache file "{cache_path}" does not have the contents it is named for (it will be written again)')
            return None
        lru_cache_mark_used(cache_path)
        return cache_path

    def read(self, s3_object_metadata: S3ObjectMetadata) -> Union[bytes, None]:
        """
        Read an S3 object's contents from the cache.

        :param s3_object_metadata: S3 object metadata
        :return: the contents, or None if they are not in the cache
        """
        if (cache_path := self.find(s3_object_metadata)) is None:
            return None
        try:
            return cache_path.read_bytes()
        except FileNotFoundError as e:
            log.info(f"{cache_path=} {e}")  # e.g. another process evicted it from the cache after it was found
            return None

    def copy_to(self, s3_object_metadata: S3ObjectMetadata, dest_path: Path) -> bool:
        """
        Copy an S3 object's contents from the cache to a file, which gets the modification time of the S3 object (the cached contents may have been downloaded from
        another S3 object with the same contents).

        :param s3_object_metadata: S3 object metadata
        :param dest_path: destination file path
        :return: True if copied, False if the contents are not in the cache
        """
        if (cache_path := self.find(s3_object_metadata)) is None:
            return False
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            shutil.copyfile(cache_path, dest_path)
        except FileNotFoundError as e:
            log.info(f"{cache_path=} {e}")  # e.g. another process evicted it from the cache after it was found
            return False
        set_file_mtime(dest_path, s3_object_metadata.mtime)
        return True

    def write(self, s3_object_metadata: S3ObjectMetadata, new_data: Union[Path, bytes], new_data_sha512: str) -> bool:
        """
        Write the contents of an S3 object that were just downloaded to the cache, in a file named with the SHA-512 of the contents. Raises AWSimpleException if the
        S3 object's SHA-512 is not the SHA-512 of the contents (S3 does not validate object metadata, so contents are only cached with the SHA-512 they have).

        :param s3_object_metadata: S3 object metadata from before the download
        :param new_data: the downloaded contents (a file path or bytes)
        :param new_data_sha512: SHA-512 calculated from the downloaded contents
        :return: True if written to the cache (False is a normal outcome, e.g. the contents are larger than the cache)
        """
        content_sha512 = s3_object_metadata.get_content_sha512()
        if content_sha512 is not None and content_sha512 != new_data_sha512:
            raise AWSimpleException(
                f"{s3_object_metadata.bucket}/{s3_object_metadata.key} : the SHA-512 of the downloaded contents is {new_data_sha512} but the S3 object's SHA-512 is {content_sha512} "
                "(the S3 object was written again during the download, or the S3 object's metadata is wrong)"
            )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if not lru_cache_write(new_data, self.cache_dir, new_data_sha512, self.max_absolute_size, self.max_free_portion):
            return False
        if content_sha512 is None:
            # the S3 object has no SHA-512, so record the SHA-512 awsimple calculated for it
            index_path = self._get_index_path(s3_object_metadata)
            index_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with atomic_write_path(index_path) as temp_path:
                    temp_path.write_text(new_data_sha512)
            except PermissionError as e:
                # e.g. on Windows, another process has the index file open. The contents are not found in the cache next time, which is a normal outcome of a cache write.
                log.info(f"{index_path=} {e}")
                return False
        return True
