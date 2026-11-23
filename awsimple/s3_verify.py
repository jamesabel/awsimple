"""
The parts of a verification of a local directory against a bucket (see S3Access.verify_directory()) that do not access S3: the result types, the plan (which S3 objects
have a local file to verify), the hash a local file is compared with, and the check of one local file against the metadata of its S3 object.

A local file is verified against the metadata of its S3 object: its size, a hash of its contents and its modification time. The hash is the strongest hash of the whole
contents that S3 has for the object (see get_s3_object_hash()). S3 only has a checksum of an object that was written with one, and the checksum of an object that was
uploaded in parts can be of its parts ("composite"), which is not a hash of the file. An object without a hash of its whole contents is verified by size and
modification time only.
"""

import hashlib
import zlib
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Callable, Dict, List, Tuple, Union

from hashy import get_file_sha512

from .crc64nvme import file_chunk_size, get_file_crc64nvme
from .s3_object import S3ObjectMetadata, native_checksum_to_hex
from .s3_sync import S3SyncObject, S3SyncOutcome


class S3VerifyOutcome(StrEnum):
    """
    What S3Access.verify_directory() found for an S3 object.
    """

    verified = "verified"  # the local file has the size, the hash (if S3 has one of the whole contents) and the modification time of the S3 object
    differs = "differs"  # the local file is not a copy of the S3 object: it is missing, or its size or hash is not the S3 object's (S3VerifyObject.reason says which)
    mtime_differs = "mtime differs"  # the local file has the contents of the S3 object, but not its modification time (S3VerifyObject.reason has the difference)
    skipped = "skipped"  # there is no local file to verify (S3VerifyObject.reason says why), e.g. a folder marker
    failed = "failed"  # could not be verified, e.g. the metadata of the S3 object could not be had (S3VerifyObject.reason says why)
    not_attempted = "not attempted"  # a stop was requested before the verification of this object started


@dataclass
class S3VerifyObject:
    """
    An S3 object in a verification (see S3Access.verify_directory()).
    """

    key: str
    outcome: S3VerifyOutcome
    local_path: Union[Path, None] = None  # the local file of this S3 object (None if it has none)
    reason: str = ""  # why the outcome is what it is (for every outcome but verified and not attempted)
    hash_name: Union[str, None] = None  # the hash that was compared, e.g. "SHA-512". None if none was: S3 has no hash of the whole contents, or the outcome was known without it.

    def set_outcome(self, outcome: S3VerifyOutcome, reason: str = ""):
        """
        :param outcome: what the verification found for this object
        :param reason: why
        """
        self.outcome = outcome
        self.reason = reason


@dataclass
class S3VerifyResult:
    """
    The result of S3Access.verify_directory(): every object of the bucket listing (or of the sync result that was verified) with its outcome.
    """

    objects: Dict[str, S3VerifyObject] = field(default_factory=dict)  # by S3 key
    stopped: bool = False  # True if a stop was requested before every object was verified

    def get_objects(self, outcome: Union[S3VerifyOutcome, None] = None) -> List[S3VerifyObject]:
        """
        :param outcome: an outcome, or None for every object
        :return: the objects with that outcome
        """
        return [verify_object for verify_object in self.objects.values() if outcome is None or verify_object.outcome == outcome]

    def get_count(self, outcome: Union[S3VerifyOutcome, None] = None) -> int:
        """
        :param outcome: an outcome, or None for every object
        :return: number of objects with that outcome
        """
        return len(self.get_objects(outcome))


def _get_file_crc32(path: Path, crc32: Callable[[bytes, int], int]) -> str:
    """
    :param path: path to a file
    :param crc32: gives a 32 bit CRC of bytes that follow the bytes of a previous CRC, e.g. zlib.crc32
    :return: the CRC of the contents of the file, as 8 lower case hex digits (what S3 gives, once decoded from base64)
    """
    value = 0
    with open(path, "rb") as file:
        while chunk := file.read(file_chunk_size):
            value = crc32(chunk, value)
    return format(value, "08x")


def get_file_crc32(path: Path) -> str:
    """
    :param path: path to a file
    :return: the CRC32 of the contents of the file, as 8 lower case hex digits
    """
    return _get_file_crc32(path, zlib.crc32)


def get_file_crc32c(path: Path) -> str:
    """
    Get the CRC32C of the contents of a file. This needs awscrt (the AWS Common Runtime, which is installed with awsimple): Python has no CRC32C of its own, and
    there is no pure Python fallback for it here. ImportError is raised where awscrt is not available.

    :param path: path to a file
    :return: the CRC32C of the contents of the file, as 8 lower case hex digits
    """
    from awscrt import checksums  # imported here, so that awsimple itself can be imported where awscrt is not available (as crc64nvme.py allows for)

    return _get_file_crc32(path, checksums.crc32c)


def _get_file_digest(path: Path, algorithm: str) -> str:
    """
    :param path: path to a file
    :param algorithm: the name hashlib has for a hash, e.g. "sha256"
    :return: that hash of the contents of the file, as lower case hex digits
    """
    with open(path, "rb") as file:
        return hashlib.file_digest(file, algorithm).hexdigest()


def get_file_sha256(path: Path) -> str:
    """
    :param path: path to a file
    :return: the SHA-256 of the contents of the file, as lower case hex digits
    """
    return _get_file_digest(path, "sha256")


def get_file_sha1(path: Path) -> str:
    """
    :param path: path to a file
    :return: the SHA-1 of the contents of the file, as lower case hex digits
    """
    return _get_file_digest(path, "sha1")


@dataclass(frozen=True)
class S3ObjectHash:
    """
    A hash of the whole contents of an S3 object that a local file can be compared with.
    """

    name: str  # e.g. "SHA-512"
    value: str  # the hash of the S3 object's contents, as lower case hex digits
    get_file_hash: Callable[[Path], str]  # gives this hash of the contents of a file, as lower case hex digits


# S3's native checksums that are not a SHA-512 (see get_s3_object_hash()), the strongest first: the name of the hash, the name it has in S3ObjectMetadata.checksums, and
# what calculates it for a file. A SHA detects contents that were made to differ, and a CRC a damaged or incomplete file.
_native_hashes: List[Tuple[str, str, Callable[[Path], str]]] = [
    ("SHA-256", "ChecksumSHA256", get_file_sha256),
    ("SHA-1", "ChecksumSHA1", get_file_sha1),
    ("CRC64NVME", "ChecksumCRC64NVME", get_file_crc64nvme),
    ("CRC32C", "ChecksumCRC32C", get_file_crc32c),
    ("CRC32", "ChecksumCRC32", get_file_crc32),
]


def get_s3_object_hash(s3_object_metadata: S3ObjectMetadata) -> Union[S3ObjectHash, None]:
    """
    Get the strongest hash of the whole contents that S3 has for an object: its SHA-512 (S3's native full-object SHA-512, or the SHA-512 awsimple writes as object
    metadata, see S3ObjectMetadata.get_content_sha512()), otherwise S3's native full-object SHA-256, SHA-1, CRC64NVME, CRC32C or CRC32 checksum. A composite checksum
    (of the parts of an object that was uploaded in parts) is not a hash of the whole contents.

    :param s3_object_metadata: the metadata of the S3 object
    :return: the hash, or None if S3 has no hash of the whole contents of the object
    """
    if (sha512 := s3_object_metadata.get_content_sha512()) is not None:
        return S3ObjectHash("SHA-512", sha512, get_file_sha512)
    for name, checksum_name, get_file_hash in _native_hashes:
        if (value := native_checksum_to_hex(s3_object_metadata.checksums.get(checksum_name))) is not None:
            return S3ObjectHash(name, value, get_file_hash)
    return None


def _get_contents_difference(verify_object: S3VerifyObject, local_path: Path, local_size: int, s3_object_metadata: S3ObjectMetadata) -> str:
    """
    Compare the contents of a local file with an S3 object: the size, and if the size is the same, the hash (which is then put in verify_object.hash_name).

    :param verify_object: the object that is verified
    :param local_path: path to the local file
    :param local_size: size of the local file in bytes
    :param s3_object_metadata: the metadata of the S3 object
    :return: how the contents differ ("" if they do not)
    """
    if local_size != s3_object_metadata.size:
        # the contents differ, which a hash would only say again
        return f"the local file is {local_size} bytes but the S3 object is {s3_object_metadata.size} bytes"
    if (s3_object_hash := get_s3_object_hash(s3_object_metadata)) is None:
        return ""
    local_hash = s3_object_hash.get_file_hash(local_path)
    verify_object.hash_name = s3_object_hash.name
    if local_hash != s3_object_hash.value:
        return f"the {s3_object_hash.name} of the local file is {local_hash} but the S3 object's is {s3_object_hash.value}"
    return ""


def verify_local_file(verify_object: S3VerifyObject, s3_object_metadata: S3ObjectMetadata, mtime_abs_tol: float):
    """
    Verify the local file of an S3 object against the metadata of the S3 object, and put the outcome in the object: differs if the file is missing or its size or hash
    is not the S3 object's, otherwise mtime differs if its modification time is not the S3 object's, otherwise verified. A local file that can not be read is failed.

    :param verify_object: the object to verify, which has the path of its local file
    :param s3_object_metadata: the metadata of the S3 object
    :param mtime_abs_tol: modification times within this many seconds are taken as the same (S3 and file systems keep times to different resolutions)
    """
    local_path = verify_object.local_path
    assert local_path is not None
    try:
        local_stat = local_path.stat()
    except OSError as e:
        verify_object.set_outcome(S3VerifyOutcome.differs, f'no local file "{local_path}" ({e})')
        return
    try:
        contents_difference = _get_contents_difference(verify_object, local_path, local_stat.st_size, s3_object_metadata)
    except OSError as e:
        verify_object.set_outcome(S3VerifyOutcome.failed, f'could not read the local file "{local_path}" ({e})')
        return

    mtime_difference = local_stat.st_mtime - s3_object_metadata.mtime.timestamp()
    if len(contents_difference) > 0:
        verify_object.set_outcome(S3VerifyOutcome.differs, contents_difference)
    elif abs(mtime_difference) > mtime_abs_tol:
        verify_object.set_outcome(S3VerifyOutcome.mtime_differs, f"the local file was modified {mtime_difference:+.0f} seconds from the S3 object ({s3_object_metadata.mtime.isoformat()})")
    else:
        verify_object.set_outcome(S3VerifyOutcome.verified)


def plan_verify(sync_objects: Dict[str, S3SyncObject], verified_outcomes: Tuple[S3SyncOutcome, ...]) -> Dict[str, S3VerifyObject]:
    """
    Determine which S3 objects have a local file to verify, from the plan or the result of a sync (see plan_sync() and S3Access.sync_to_directory()): an object the
    sync has one of the given outcomes for is to be verified, and any other object is skipped, with what the sync says about it as the reason.

    :param sync_objects: the objects of a sync plan or of a sync result, by S3 key
    :param verified_outcomes: the sync outcomes of an object that has a local file to verify
    :return: verify objects by S3 key, in the order given. The outcome of the objects to verify is S3VerifyOutcome.not_attempted.
    """
    verify_objects = {}
    for s3_key, sync_object in sync_objects.items():
        if sync_object.outcome in verified_outcomes and sync_object.local_path is not None:
            verify_objects[s3_key] = S3VerifyObject(s3_key, S3VerifyOutcome.not_attempted, sync_object.local_path)
        else:
            reason = f"sync outcome is {sync_object.outcome}" + (f" : {sync_object.reason}" if len(sync_object.reason) > 0 else "")
            verify_objects[s3_key] = S3VerifyObject(s3_key, S3VerifyOutcome.skipped, sync_object.local_path, reason)
    return verify_objects
