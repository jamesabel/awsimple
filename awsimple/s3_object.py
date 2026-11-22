"""
What awsimple knows about an S3 object: its metadata, its hashes, and what a bucket listing gives for it.

awsimple's hashes are hashes of the whole file. They are the same whether the object was uploaded in one part or in several:

- CRC64NVME: S3's native full-object checksum, which S3 validates when the object is written. It is what the length and contents of every transfer are checked against.
  A CRC is not collision resistant, so it is never used on its own to say that two contents are the same.
- SHA-512: the hash that definitively represents the contents. awsimple writes it as object metadata (see sha512_string) on every object it writes, since an S3 object
  has only one native checksum. An object written by another tool may instead have it as S3's native full-object checksum.
"""

import base64
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Union

from hashy import get_dls_sha512
from typeguard import typechecked

from .__version__ import __application_name__

# The key of the S3 object metadata that holds the SHA-512 of the object's contents (as a hex string). The project's name is a prefix to avoid collisions with other metadata,
# with a dash since that is AWS's convention.
sha512_string = f"{__application_name__}-sha512"

# a SHA-512 as a hex string. Object metadata is whatever its writer set, so a value is only taken as a SHA-512 (and used as a file name in the cache) if it matches this.
sha512_hex_regex = re.compile(r"[0-9a-f]{128}")

# Part of every S3 identity hash (see S3ObjectMetadata.get_identity_sha512()). Change it whenever the fields in the identity hash change, so that hashes made the old way are never matched.
cache_key_scheme = f"{__application_name__}-s3-cache-key-2"


@typechecked()
def native_checksum_to_hex(checksum_base64: Union[str, None]) -> Union[str, None]:
    """
    Convert an S3 native checksum (base64 encoded) to a hex string comparable with locally computed hashes.
    Composite (multipart) checksums have a "-<part count>" suffix and do not represent the whole object, so they convert to None.

    :param checksum_base64: base64 encoded checksum from S3 (or None)
    :return: hex string, or None if no full-object checksum available
    """
    if checksum_base64 is None or "-" in checksum_base64:
        return None
    return base64.b64decode(checksum_base64).hex()


@typechecked()
def set_file_mtime(file_path: Path, mtime: datetime):
    """
    Set the modification time of a local file to the modification time of the S3 object it was read as. Every file awsimple downloads, or copies from its cache, gets this,
    so that a local file can be compared with its S3 object by modification time.

    :param file_path: local file path
    :param mtime: modification time of the S3 object
    """
    mtime_timestamp = mtime.timestamp()
    os.utime(file_path, (mtime_timestamp, mtime_timestamp))


@dataclass
class S3ListedObject:
    """
    An S3 object as a bucket listing gives it (see S3Access.list_objects()). A bucket listing does not have an object's checksums or its object metadata: those are in
    S3ObjectMetadata (see S3Access.get_s3_object_metadata()), which takes a request per object.
    """

    key: str
    size: int  # bytes
    mtime: datetime
    etag: str  # without the quotes S3 gives it in, lower case
    storage_class: str  # e.g. "STANDARD", or "GLACIER" or "DEEP_ARCHIVE" for an object that has to be restored before it can be read


@dataclass
class S3DownloadStatus:
    """
    What a cached download did (see S3Access.download_cached() and S3Access.download_object_as_json_cached()).
    """

    success: bool = False
    cache_hit: Union[bool, None] = None  # True if the contents came from the cache
    cache_write: Union[bool, None] = None  # True if the downloaded contents were written to the cache (None on a cache hit)


@dataclass
class S3ObjectMetadata:
    """
    The metadata of an S3 object (see S3Access.get_s3_object_metadata()), including its hashes.
    """

    bucket: str
    key: str
    size: int  # bytes
    mtime: datetime
    etag: str  # without the quotes S3 gives it in, lower case. Not a hash of the contents (e.g. for a multipart upload), so generally not used.
    sha512: Union[
        str, None
    ]  # hex string - S3's native full-object SHA-512 checksum (None if the object doesn't have one, e.g. objects written by awsimple, whose native checksum is CRC64NVME)
    url: str  # URL of S3 object
    metadata_sha512: Union[str, None] = None  # hex string - the SHA-512 in the object metadata (see sha512_string), as written. awsimple writes it on every object it writes.
    crc64nvme: Union[str, None] = None  # hex string - S3's native full-object CRC64NVME checksum (None if the object doesn't have one). awsimple writes every object with it.
    version_id: Union[str, None] = None  # S3 version ID (None if the bucket is not versioned)
    checksum_type: Union[str, None] = None  # S3's native checksum type: "FULL_OBJECT" or "COMPOSITE" (None if the object has no native checksum)
    checksums: Dict[str, str] = field(default_factory=dict)  # every native checksum S3 returned, as returned (base64, composite included), by response field name, e.g. "ChecksumCRC64NVME"

    def get_content_sha512(self) -> Union[str, None]:
        """
        Get the SHA-512 of this object's contents from S3's metadata: S3's native full-object SHA-512 if the object has one, otherwise the SHA-512 in the object metadata.

        :return: SHA-512 (as hex string), or None if the object has neither (or the object metadata value is not a SHA-512)
        """
        if self.sha512 is not None:
            return self.sha512
        if self.metadata_sha512 is not None and sha512_hex_regex.fullmatch(self.metadata_sha512) is not None:
            return self.metadata_sha512
        return None

    def get_identity_sha512(self) -> str:
        """
        Get a SHA-512 of this object's identity (bucket, key, version) and of S3's native metadata that changes when the contents do (size, modification time, ETag and every
        native checksum). This is not a hash of the object's contents. It is what is initially used to find a cached object that has no SHA-512 of its contents: awsimple never
        updates an S3 object's metadata on a read, so such an object is checked against its native CRC64NVME, key, size and modification time, which are all in this hash.
        The key is only ever hashed, never used as a file name, so file system path irregularities (e.g. leading or doubled slashes, case-insensitive or reserved names,
        characters that are not allowed in a file name) can not make two keys the same or make a key unusable.

        A native checksum alone is not used since a CRC (e.g. CRC64NVME, which awsimple's uploads use) is not collision resistant: different contents with the same CRC are easy
        to make. With the identity in the hash, two different objects only have the same identity hash if SHA-512 itself collides.

        :return: SHA-512 (as hex string)
        """
        identity_fields = {
            "scheme": cache_key_scheme,
            "bucket": self.bucket,
            "key": self.key,
            "version_id": self.version_id,
            "size": self.size,
            "mtime": int(round(self.mtime.timestamp())),  # round timestamp to seconds to try to avoid possible small deltas when dealing with time and floats
            "etag": self.etag,
            "sha512": self.sha512,
            "crc64nvme": self.crc64nvme,
            "checksum_type": self.checksum_type,
            "checksums": self.checksums,
        }
        return get_dls_sha512(identity_fields)

    def get_sha512(self) -> str:
        """
        Get the SHA-512 of this object's contents if S3's metadata has it (see get_content_sha512()), otherwise the identity hash (see get_identity_sha512()).

        :return: SHA-512 (as hex string)
        """
        if (sha512_value := self.get_content_sha512()) is None:
            sha512_value = self.get_identity_sha512()
        return sha512_value
