"""
The parts of a bucket-to-directory sync (see S3Access.sync_to_directory()) that do not access S3: the result types, and the plan, which decides what a sync does with
every listed S3 object from the bucket listing and the local files alone.
"""

import stat
from collections import Counter
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Dict, List, Union

from s3transfer.subscribers import BaseSubscriber

from .exceptions import AWSimpleException
from .s3_object import S3ListedObject, S3ObjectMetadata
from .s3_paths import local_path_collision_name, s3_key_to_local_path

# Storage classes of objects that have to be restored before they can be read. A sync skips them.
archived_storage_classes = ("GLACIER", "DEEP_ARCHIVE")


class S3SyncOutcome(StrEnum):
    """
    What S3Access.sync_to_directory() did with an S3 object.
    """

    downloaded = "downloaded"
    would_download = "would download"  # a dry run: the object is one that a sync downloads
    up_to_date = "up to date"  # the local file is already current
    skipped = "skipped"  # not downloaded on purpose (S3SyncObject.reason says why)
    failed = "failed"  # could not be downloaded (S3SyncObject.reason says why)
    not_attempted = "not attempted"  # a stop was requested before the download started


@dataclass
class S3SyncObject:
    """
    An S3 object in a sync (see S3Access.sync_to_directory()).
    """

    key: str
    size: int  # bytes
    outcome: S3SyncOutcome
    local_path: Union[Path, None] = None  # the local file of this S3 object (None if its key can not be a local file)
    reason: str = ""  # why the object was skipped or failed

    def set_outcome(self, outcome: S3SyncOutcome, reason: str = ""):
        """
        :param outcome: what the sync did with this object
        :param reason: why (for skipped and failed)
        """
        self.outcome = outcome
        self.reason = reason


@dataclass
class S3SyncResult:
    """
    The result of S3Access.sync_to_directory(): every object of the bucket listing with its outcome.
    """

    objects: Dict[str, S3SyncObject] = field(default_factory=dict)  # by S3 key, in the order of the bucket listing
    stopped: bool = False  # True if a stop was requested before every download had started

    def get_objects(self, outcome: Union[S3SyncOutcome, None] = None) -> List[S3SyncObject]:
        """
        :param outcome: an outcome, or None for every listed object
        :return: the objects with that outcome
        """
        return [sync_object for sync_object in self.objects.values() if outcome is None or sync_object.outcome == outcome]

    def get_count(self, outcome: Union[S3SyncOutcome, None] = None) -> int:
        """
        :param outcome: an outcome, or None for every listed object
        :return: number of objects with that outcome
        """
        return len(self.get_objects(outcome))

    def get_size(self, outcome: Union[S3SyncOutcome, None] = None) -> int:
        """
        :param outcome: an outcome, or None for every listed object
        :return: total size in bytes of the objects with that outcome
        """
        return sum(sync_object.size for sync_object in self.get_objects(outcome))


class ProvideMetadataSubscriber(BaseSubscriber):
    """
    Gives a transfer the size and the ETag of the S3 object, so that the transfer manager does not make its own request for them. The transfer manager uses the ETag to
    fail a download in parts if the S3 object is written again during the download.
    """

    def __init__(self, s3_object_metadata: S3ObjectMetadata):
        """
        :param s3_object_metadata: metadata of the S3 object that is downloaded
        """
        self.s3_object_metadata = s3_object_metadata

    def on_queued(self, future, **kwargs):
        """
        Called by the transfer manager when the transfer is queued (before it would request the metadata).
        """
        future.meta.provide_transfer_size(self.s3_object_metadata.size)
        future.meta.provide_object_etag(f'"{self.s3_object_metadata.etag}"')  # as S3 gives it, in quotes


def _sync_object_from_listing(listed_object: S3ListedObject, dest_dir: Path, prefix: str) -> S3SyncObject:
    """
    Make the sync object of a listed S3 object: one to download (subject to the checks that need every object, see plan_sync()), or skipped or failed with the reason.

    :param listed_object: an object of the bucket listing
    :param dest_dir: destination directory
    :param prefix: the prefix of the listing, which is not part of the local path
    :return: sync object
    """
    sync_object = S3SyncObject(listed_object.key, listed_object.size, S3SyncOutcome.would_download)
    if listed_object.key.endswith("/"):
        sync_object.set_outcome(S3SyncOutcome.skipped, 'the key ends with "/" (a folder marker, not a file)')
    elif listed_object.storage_class in archived_storage_classes:
        sync_object.set_outcome(S3SyncOutcome.skipped, f"storage class {listed_object.storage_class} has to be restored before it can be read")
    else:
        try:
            sync_object.local_path = s3_key_to_local_path(dest_dir, listed_object.key[len(prefix) :])
        except AWSimpleException as e:
            sync_object.set_outcome(S3SyncOutcome.failed, str(e))
    return sync_object


def is_local_file_current(local_path: Path, listed_object: S3ListedObject, size_only: bool, mtime_abs_tol: float) -> bool:
    """
    Determine if a local file is current with an S3 object: it is a file of the same size, and (unless size_only) the S3 object is not newer.

    :param local_path: local file path
    :param listed_object: the S3 object, from the bucket listing
    :param size_only: True to not compare modification times
    :param mtime_abs_tol: modification times within this many seconds are taken as the same
    :return: True if the local file is current
    """
    try:
        local_stat = local_path.stat()
    except OSError:
        # no such file, or a path the OS does not take (a download to it is then reported as failed)
        return False
    if not stat.S_ISREG(local_stat.st_mode) or local_stat.st_size != listed_object.size:
        return False
    return size_only or listed_object.mtime.timestamp() <= local_stat.st_mtime + mtime_abs_tol


def plan_sync(listed_objects: Dict[str, S3ListedObject], dest_dir: Path, prefix: str, size_only: bool, mtime_abs_tol: float) -> Dict[str, S3SyncObject]:
    """
    Determine what a sync does with every listed S3 object, from the bucket listing and the local files only (no request per object).

    Keys that can not all be local files are skipped, never resolved by picking one: every key of a group that is the same local file, and a key that is a file where
    other keys need a directory.

    :param listed_objects: the bucket listing
    :param dest_dir: destination directory
    :param prefix: the prefix of the listing, which is not part of the local paths
    :param size_only: True to not compare modification times
    :param mtime_abs_tol: modification times within this many seconds are taken as the same
    :return: sync objects by S3 key, in the order of the listing. The outcome of the objects to download is S3SyncOutcome.would_download.
    """
    sync_objects = {s3_key: _sync_object_from_listing(listed_object, dest_dir, prefix) for s3_key, listed_object in listed_objects.items()}

    relative_paths = {s3_key: sync_object.local_path.relative_to(dest_dir) for s3_key, sync_object in sync_objects.items() if sync_object.local_path is not None}
    file_names = Counter(local_path_collision_name(relative_path) for relative_path in relative_paths.values())
    directory_names = {local_path_collision_name(parent) for relative_path in relative_paths.values() for parent in relative_path.parents}

    for s3_key, relative_path in relative_paths.items():
        sync_object = sync_objects[s3_key]
        local_path = Path(dest_dir, relative_path)
        name = local_path_collision_name(relative_path)
        if file_names[name] > 1:
            sync_object.set_outcome(S3SyncOutcome.skipped, f'more than one key is the local file "{local_path}"')
        elif name in directory_names:
            sync_object.set_outcome(S3SyncOutcome.skipped, f'other keys need "{local_path}" to be a directory')
        elif is_local_file_current(local_path, listed_objects[s3_key], size_only, mtime_abs_tol):
            sync_object.set_outcome(S3SyncOutcome.up_to_date)
    return sync_objects
