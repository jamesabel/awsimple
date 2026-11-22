"""
S3 access: S3Access, a simple interface to one S3 bucket.

The S3 code is in these modules:

- s3.py (this module): S3Access, which does every access to S3.
- s3_object.py: what awsimple knows about an S3 object (S3ObjectMetadata, S3ListedObject) and its hashes.
- s3_paths.py: S3 keys as local file paths (s3_key_to_local_path()).
- s3_cache.py: the local cache of S3 object contents (S3ContentCache).
- s3_sync.py: the plan and the result types of a bucket-to-directory sync.

The names that were in this module before it was split up can still be imported from it.
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from math import isclose
from pathlib import Path
from threading import Event
from typing import Any, Callable, Dict, Iterator, List, Tuple, Type, Union
import json
from logging import getLogger

from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError, SSLError
from boto3.s3.transfer import TransferConfig
from s3transfer import S3UploadFailedError
from s3transfer.exceptions import RetriesExceededError, S3DownloadFailedError
from s3transfer.futures import NonThreadedExecutor
from s3transfer.manager import TransferManager
import urllib3.exceptions
from typeguard import typechecked
from hashy import get_file_sha512, get_bytes_sha512, get_file_crc64nvme, get_bytes_crc64nvme
from yasf import sf

from awsimple import (
    CacheAccess,
    __application_name__,
    atomic_write_path,
    AWSimpleException,
    convert_serializable_special_cases,
    S3BucketAlreadyExistsNotOwnedByYou,
    boto_error_to_string,
)
from .exceptions import BucketNotFound
from .s3_cache import S3ContentCache, cache_index_dir_name
from .s3_object import S3DownloadStatus, S3ListedObject, S3ObjectMetadata, cache_key_scheme, native_checksum_to_hex, set_file_mtime, sha512_hex_regex, sha512_string
from .s3_paths import case_insensitive_file_names, s3_key_to_local_path
from .s3_sync import ProvideMetadataSubscriber, S3SyncObject, S3SyncOutcome, S3SyncResult, archived_storage_classes, plan_sync

# the public names of the S3 modules, so that they can all be imported from this module
__all__ = [
    "BucketNotFound",
    "S3Access",
    "S3ContentCache",
    "S3DownloadStatus",
    "S3ListedObject",
    "S3ObjectMetadata",
    "S3SyncObject",
    "S3SyncOutcome",
    "S3SyncResult",
    "archived_storage_classes",
    "cache_index_dir_name",
    "cache_key_scheme",
    "case_insensitive_file_names",
    "connection_errors",
    "default_multipart_threshold",
    "json_extension",
    "s3_key_to_local_path",
    "serializable_object_to_json_as_bytes",
    "sha512_hex_regex",
    "sha512_string",
]

# Uploads at or above this size use multipart (the boto3 default threshold). awsimple's hashes do not depend on this: its native checksum is CRC64NVME, which is the only
# checksum family S3 computes as a full-object value for both single-part and multipart uploads (SHA-family multipart checksums are composite and can't be compared to a
# local file hash). It is also S3's own default, so objects written by other modern tools are usually comparable too.
default_multipart_threshold = 8 * 1024 * 1024  # 8 MiB

json_extension = ".json"

log = getLogger(__application_name__)

# errors of an S3 transfer that may not happen again, so the transfer is tried again (see S3Access._retry())
connection_errors = (S3UploadFailedError, ClientError, EndpointConnectionError, SSLError, urllib3.exceptions.ProtocolError, ConnectionClosedError)


@typechecked()
def serializable_object_to_json_as_bytes(json_serializable_object: Union[List, Dict]) -> bytes:
    """
    Convert a JSON serializable Python object to the bytes awsimple writes to S3 for it.

    :param json_serializable_object: serializable object
    :return: JSON as UTF-8 bytes
    """
    return bytes(json.dumps(json_serializable_object, default=convert_serializable_special_cases).encode("UTF-8"))


def _get_json_key(s3_key: str):
    """
    get JSON key given an s3_key that may not have the .json extension
    :param s3_key: s3 key, potentially without the extension
    :return: JSON S3 key
    """
    if not s3_key.endswith(json_extension):
        s3_key = f"{s3_key}{json_extension}"
    return s3_key


class S3Access(CacheAccess):
    """
    Access to one S3 bucket: write, read, upload, download (optionally through a local cache), list and sync.

    Every object awsimple writes gets S3's native full-object CRC64NVME checksum and the SHA-512 of its contents as object metadata. Every access checks the length and the
    CRC64NVME (when the object has one) of what was transferred, and raises AWSimpleException on a mismatch.
    """

    @typechecked()
    def __init__(self, bucket_name: Union[str, None] = None, **kwargs):
        """
        S3 Access

        :param bucket_name: S3 bucket name
        :param kwargs: kwargs
        """
        self.bucket_name = bucket_name
        self.retry_sleep_time = 3.0  # seconds
        self.retry_count = 10
        self.public_readable = False
        self.download_status = S3DownloadStatus()
        self._bucket_region = None  # type: Union[str, None]  # lazily determined and cached
        super().__init__(resource_name="s3", **kwargs)

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # configuration
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def _upload_extra_args(self, content_sha512: str) -> Dict[str, Any]:
        """
        Arguments for writing an S3 object: the native full-object CRC64NVME checksum, the SHA-512 of the contents as object metadata and an optional public-read ACL.

        :param content_sha512: SHA-512 of the contents being written (as hex string)
        :return: arguments dict (ExtraArgs for an upload, keyword arguments for a put)
        """
        # S3 validates the data against the (SDK computed) checksum before storing
        extra_args: Dict[str, Any] = {"ChecksumAlgorithm": "CRC64NVME", "Metadata": {sha512_string: content_sha512}}
        if self.public_readable:
            extra_args["ACL"] = "public-read"
        return extra_args

    def get_s3_transfer_config(self) -> TransferConfig:
        """
        The transfer configuration for uploads. A derived class can override this if a different configuration is desired. The hashes awsimple writes do not depend on it.

        :return: transfer configuration
        """
        # workaround threading issue https://github.com/boto/s3transfer/issues/197
        s3_transfer_config = TransferConfig(use_threads=False, multipart_threshold=default_multipart_threshold)
        return s3_transfer_config

    def get_s3_download_transfer_config(self) -> TransferConfig:
        """
        The transfer configuration for downloads. A derived class can override this if a different configuration is desired.

        :return: transfer configuration
        """
        # This is boto3's default (threads, 10 concurrent requests, 8 MiB multipart threshold and chunk size), which is what downloads have always used.
        return TransferConfig()

    @typechecked()
    def set_public_readable(self, public_readable: bool):
        """
        Make the objects this instance writes from now on (and a bucket it creates) readable by anyone.

        :param public_readable: True for public-read
        """
        self.public_readable = public_readable

    def _get_content_cache(self) -> S3ContentCache:
        """
        :return: the local cache of S3 object contents, with the cache directory and limits this instance has now
        """
        return S3ContentCache(self.cache_dir, self.cache_max_absolute, self.cache_max_of_free)

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # common parts of S3 accesses
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def _retry(self, attempt: Callable[[int], bool], description: str, retry_on: Tuple[Type[BaseException], ...] = connection_errors):
        """
        Try an S3 transfer until it is successful, with a sleep between attempts. Raises AWSimpleException if every attempt fails.

        :param attempt: does one attempt, given the attempt number (from 0), and returns True if it was successful. An exception in retry_on is also a failed attempt.
        :param description: what is attempted, for the log and for the exception, e.g. "upload a.txt to bucket/a.txt"
        :param retry_on: the exceptions after which the transfer is tried again
        """
        for attempt_number in range(self.retry_count):
            try:
                if attempt(attempt_number):
                    return
                log.warning(f"{description} : {attempt_number=} : not successful")
            except retry_on as e:
                # e.g. ProtocolError can happen for a broken connection
                log.warning(f"{description} : {attempt_number=} : {e}")
            if attempt_number + 1 < self.retry_count:
                time.sleep(self.retry_sleep_time)
        raise AWSimpleException(f"couldn't {description} after {self.retry_count} attempts")

    def _list_contents(self, prefix: str) -> Iterator[Dict[str, Any]]:
        """
        List this S3 bucket (every page of the listing). Raises BucketNotFound if the bucket does not exist.

        :param prefix: only list objects that have this prefix in their keys ("" for all objects)
        :return: the "Contents" entries of the bucket listing, one per object
        """
        if not self.bucket_exists():
            raise BucketNotFound(self.bucket_name)
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
            yield from page.get("Contents", [])  # an empty bucket has no "Contents"

    def _read_object(self, s3_key: str) -> bytes:
        """
        Read the contents of an S3 object, and check the length and the CRC64NVME (if the S3 object has one) of what was received.
        Raises AWSimpleException if they are not the S3 object's.

        :param s3_key: S3 key
        :return: S3 object contents
        """
        assert self.resource is not None
        response = self.resource.Object(self.bucket_name, s3_key).get(ChecksumMode="ENABLED")
        body = response["Body"].read()
        if len(body) != response["ContentLength"]:
            raise AWSimpleException(f"{self.bucket_name}/{s3_key} : read {len(body)} bytes but the S3 object is {response['ContentLength']} bytes")
        s3_crc64nvme = native_checksum_to_hex(response.get("ChecksumCRC64NVME"))
        if s3_crc64nvme is not None and (body_crc64nvme := get_bytes_crc64nvme(body)) != s3_crc64nvme:
            raise AWSimpleException(f"{self.bucket_name}/{s3_key} : the CRC64NVME of what was read is {body_crc64nvme} but the S3 object's is {s3_crc64nvme}")
        return body

    def _write_object(self, data: bytes, s3_key: str, data_sha512: str, data_crc64nvme: str, retry: bool):
        """
        Write bytes to an S3 object, and check that the S3 object has the length and the CRC64NVME of what was written. Raises AWSimpleException if it does not.

        :param data: the contents to write
        :param s3_key: S3 key
        :param data_sha512: SHA-512 of the contents (as hex string)
        :param data_crc64nvme: CRC64NVME of the contents (as hex string)
        :param retry: True to try again after a connection error
        """
        assert self.resource is not None
        s3_object = self.resource.Object(self.bucket_name, s3_key)

        def _put(_unused_attempt_number: int = 0) -> bool:
            s3_object.put(Body=data, **self._upload_extra_args(data_sha512))
            return True

        if retry:
            self._retry(_put, f"upload to {self.bucket_name}/{s3_key}")
        else:
            _put()
        self._check_written_object(s3_key, len(data), data_crc64nvme)

    def _check_written_object(self, s3_key: str, expected_size: int, expected_crc64nvme: str):
        """
        Check that an S3 object that was just written has the length and the CRC64NVME of what was written. Raises AWSimpleException if it does not.

        :param s3_key: S3 key
        :param expected_size: size of what was written in bytes
        :param expected_crc64nvme: CRC64NVME of what was written (as hex string)
        """
        s3_object_metadata = self.get_s3_object_metadata(s3_key)
        if s3_object_metadata.size != expected_size:
            raise AWSimpleException(f"{self.bucket_name}/{s3_key} : wrote {expected_size} bytes but the S3 object is {s3_object_metadata.size} bytes")
        if s3_object_metadata.crc64nvme != expected_crc64nvme:
            raise AWSimpleException(f"{self.bucket_name}/{s3_key} : the CRC64NVME of what was written is {expected_crc64nvme} but the S3 object's is {s3_object_metadata.crc64nvme}")

    def _is_download_intact(self, dest_path: Path, s3_object_metadata: S3ObjectMetadata) -> bool:
        """
        Check that a file that was just downloaded has the length and the CRC64NVME (if the S3 object has one) of the S3 object.

        :param dest_path: path to the downloaded file
        :param s3_object_metadata: S3 object metadata
        :return: True if the file has the S3 object's length and CRC64NVME
        """
        if (dest_size := os.path.getsize(dest_path)) != s3_object_metadata.size:
            log.warning(f"{self.bucket_name}/{s3_object_metadata.key} to {dest_path} : downloaded {dest_size} bytes but the S3 object is {s3_object_metadata.size} bytes")
            return False
        if s3_object_metadata.crc64nvme is not None and (dest_crc64nvme := get_file_crc64nvme(dest_path)) != s3_object_metadata.crc64nvme:
            log.warning(f"{self.bucket_name}/{s3_object_metadata.key} to {dest_path} : the downloaded CRC64NVME is {dest_crc64nvme} but the S3 object's is {s3_object_metadata.crc64nvme}")
            return False
        return True

    def _finish_download(self, file_path: Path, s3_object_metadata: S3ObjectMetadata) -> bool:
        """
        The last step of every download: check the downloaded file against the S3 object (see _is_download_intact()) and, if it is intact, give it the modification time
        of the S3 object.

        :param file_path: path to the downloaded file
        :param s3_object_metadata: S3 object metadata
        :return: True if the file is intact
        """
        if not self._is_download_intact(file_path, s3_object_metadata):
            return False
        set_file_mtime(file_path, s3_object_metadata.mtime)
        return True

    def _is_upload_needed(self, s3_key: str, size: int, crc64nvme: str, sha512: str, mtime: Union[float, None] = None) -> bool:
        """
        Determine if contents differ from the S3 object they would be written to. The hashes compared are of the whole contents, so this does not depend on how either
        was (or will be) uploaded.

        :param s3_key: S3 key
        :param size: size of the contents in bytes
        :param crc64nvme: CRC64NVME of the contents (as hex string)
        :param sha512: SHA-512 of the contents (as hex string)
        :param mtime: modification time of the contents (None if they have none, e.g. they are not a file)
        :return: True if the S3 object does not exist, is not known to have these contents, or would gain a checksum or SHA-512 it does not have
        """
        if not self.object_exists(s3_key):
            return True

        s3_object_metadata = self.get_s3_object_metadata(s3_key)
        log.info(f"{s3_object_metadata=}")
        if s3_object_metadata.crc64nvme is not None:
            # Use the native full-object checksum, if the object has one. A CRC is not collision resistant, so the size and the SHA-512 must also be the same. An object
            # that has no SHA-512 is written again so that it gains one (every S3 object awsimple writes has its SHA-512, which is what the cache finds it by).
            return not (crc64nvme == s3_object_metadata.crc64nvme and size == s3_object_metadata.size and sha512 == s3_object_metadata.get_content_sha512())
        if s3_object_metadata.sha512 is not None:
            # the object was written by another tool with a native full-object SHA-512 checksum
            return sha512 != s3_object_metadata.sha512
        if s3_object_metadata.metadata_sha512 is not None or mtime is None:
            # No native checksum, so the object is always written again so that it gains one: it has the metadata SHA-512 only (e.g. written by an older awsimple),
            # or there is nothing else to compare.
            return True
        # no hash is available, so compare modification time and file size (free and robust, but weaker than a hash)
        return not (size == s3_object_metadata.size and isclose(mtime, s3_object_metadata.mtime.timestamp(), abs_tol=self.mtime_abs_tol))

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # buckets
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @typechecked()
    def bucket_list(self) -> list:
        """
        list out all buckets
        (not called list_buckets() since that's used in boto3 but this returns a list of bucket strings not a list of dicts)

        :return: list of buckets
        """
        return [b["Name"] for b in self.client.list_buckets()["Buckets"]]

    @typechecked()
    def bucket_exists(self) -> bool:
        """
        Test if S3 bucket exists

        :return: True if bucket exists
        """

        # use a "custom" config so that .head_bucket() doesn't take a really long time if the bucket does not exist
        if self.is_mocked():
            s3 = self.client  # the existing client is already the mock's
        else:
            config = Config(connect_timeout=5, retries={"max_attempts": 3, "mode": "standard"})
            s3 = self.session.client("s3", config=config)  # use the session so the configured profile/keys/region are honored
        assert self.bucket_name is not None
        try:
            s3.head_bucket(Bucket=self.bucket_name)
            exists = True
        except ClientError as e:
            log.info(f"{self.bucket_name=}{e=}")
            exists = False
        return exists

    @typechecked()
    def create_bucket(self) -> bool:
        """
        create S3 bucket

        :return: True if bucket created
        """

        # this is ugly, but create_bucket needs to be told the region explicitly (it doesn't just take it from the config)
        if (region := self.get_region()) is None:
            raise RuntimeError("no region given (check ~.aws/config")
        else:
            location = {"LocationConstraint": region}

        try:
            self.client.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration=location)
            self.client.get_waiter("bucket_exists").wait(Bucket=self.bucket_name)
            if self.public_readable:
                # Enable per-object ACLs by setting ObjectOwnership to BucketOwnerPreferred (the default BucketOwnerEnforced disallows ACLs).
                # This allows mixing public and private objects in the same bucket via per-object ACL on upload.
                self.client.put_bucket_ownership_controls(
                    Bucket=self.bucket_name,
                    OwnershipControls={"Rules": [{"ObjectOwnership": "BucketOwnerPreferred"}]},
                )
                self.client.put_public_access_block(
                    Bucket=self.bucket_name,
                    PublicAccessBlockConfiguration={"BlockPublicAcls": False, "IgnorePublicAcls": False, "BlockPublicPolicy": False, "RestrictPublicBuckets": False},
                )
            created = True
        except self.client.exceptions.BucketAlreadyOwnedByYou:
            created = False  # already exists and owned by you
        except self.client.exceptions.BucketAlreadyExists as e:
            # bucket already exists and is owned by someone else
            raise S3BucketAlreadyExistsNotOwnedByYou(str(self.bucket_name)) from e
        return created

    @typechecked()
    def delete_bucket(self) -> bool:
        """
        delete S3 bucket

        :return: True if bucket deleted (False if didn't exist in the first place)
        """
        try:
            self.client.delete_bucket(Bucket=self.bucket_name)
            deleted = True
        except ClientError as e:
            log.info(f"{self.bucket_name=}{e=}")  # does not exist
            deleted = False
        return deleted

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # objects: metadata and listings
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @typechecked()
    def get_s3_object_url(self, s3_key: str) -> str:
        """
        Get S3 object URL

        :param s3_key: S3 key
        :return: object URL
        """
        if self._bucket_region is None:
            bucket_location = self.client.get_bucket_location(Bucket=self.bucket_name)
            self._bucket_region = bucket_location["LocationConstraint"] or "us-east-1"  # LocationConstraint is None for us-east-1
        url = f"https://{self.bucket_name}.s3.{self._bucket_region}.amazonaws.com/{s3_key}"
        return url

    @typechecked()
    def get_s3_object_metadata(self, s3_key: str) -> S3ObjectMetadata:
        """
        Get S3 object metadata. Raises AWSimpleException if the object does not exist.

        :param s3_key: S3 key
        :return: S3ObjectMetadata
        """
        try:
            head = self.client.head_object(Bucket=self.bucket_name, Key=s3_key, ChecksumMode="ENABLED")
        except ClientError as e:
            if boto_error_to_string(e) in ("404", "NoSuchKey", "NotFound"):
                raise AWSimpleException(f"{self.bucket_name=} {s3_key=} does not exist") from e
            raise
        assert isinstance(self.bucket_name, str)  # mainly for the type checker
        # sha512 and crc64nvme are S3's native (server-validated) full-object checksums; metadata_sha512 is the SHA-512 in awsimple's object metadata (not validated by S3)
        s3_object_metadata = S3ObjectMetadata(
            self.bucket_name,
            s3_key,
            head["ContentLength"],
            head["LastModified"],
            head["ETag"][1:-1].lower(),
            native_checksum_to_hex(head.get("ChecksumSHA512")),
            self.get_s3_object_url(s3_key),
            head.get("Metadata", {}).get(sha512_string),
            native_checksum_to_hex(head.get("ChecksumCRC64NVME")),
            head.get("VersionId"),
            head.get("ChecksumType"),
            {name: value for name, value in head.items() if name.startswith("Checksum") and name != "ChecksumType"},  # e.g. ChecksumCRC64NVME, ChecksumSHA512, ChecksumCRC32
        )
        log.debug(f"{s3_object_metadata=}")
        return s3_object_metadata

    @typechecked()
    def object_exists(self, s3_key: str) -> bool:
        """
        determine if an s3 object exists

        :param s3_key: the S3 object key
        :return: True if object exists
        """
        assert self.resource is not None
        bucket_resource = self.resource.Bucket(self.bucket_name)
        objs = list(bucket_resource.objects.filter(Prefix=s3_key))
        object_exists = len(objs) > 0 and objs[0].key == s3_key
        log.debug(f"{self.bucket_name}:{s3_key} : {object_exists=}")
        return object_exists

    @typechecked()
    def list_objects(self, prefix: str = "") -> Dict[str, S3ListedObject]:
        """
        List the objects in this S3 bucket, with what the bucket listing itself gives for each one: size, modification time, ETag and storage class.

        This makes no request per object (a listing request returns up to 1000 objects), so use it instead of .dir() when that is all you need, e.g. to compare a bucket with
        local files. Use .dir() for all of an object's metadata, including its checksums. Raises BucketNotFound if the bucket does not exist.

        :param prefix: only list objects that have this prefix in their keys (omit for all objects)
        :return: a dict where key is the S3 key and the value is S3ListedObject, in the order of the bucket listing
        """
        # an object's storage class is not given by every S3 implementation when it is the default
        return {
            content["Key"]: S3ListedObject(content["Key"], content["Size"], content["LastModified"], content["ETag"][1:-1].lower(), content.get("StorageClass", "STANDARD"))
            for content in self._list_contents(prefix)
        }

    @typechecked()
    def dir(self, prefix: str = "") -> Dict[str, S3ObjectMetadata]:
        """
        Do a "directory" of an S3 bucket where the returned dict key is the S3 key and the value is an S3ObjectMetadata object.

        This makes a request for every object. Use .list_objects() if the size, modification time, ETag and storage class are all you need.
        Use the faster .keys() method if all you need are the keys. Raises BucketNotFound if the bucket does not exist.

        :param prefix: only do a dir on objects that have this prefix in their keys (omit for all objects)
        :return: a dict where key is the S3 key and the value is S3ObjectMetadata
        """
        return {content["Key"]: self.get_s3_object_metadata(content["Key"]) for content in self._list_contents(prefix)}

    def keys(self, prefix: str = "") -> List[str]:
        """
        List all the keys in this S3 Bucket.

        Note that this should be faster than .dir() if all you need are the keys and not the metadata. Raises BucketNotFound if the bucket does not exist.

        :param prefix: only do a dir on objects that have this prefix in their keys (omit for all objects)
        :return: a sorted list of all the keys in this S3 Bucket (sorted for consistency)
        """
        return sorted(content["Key"] for content in self._list_contents(prefix))

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # objects: write and read
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @typechecked()
    def read_string(self, s3_key: str) -> str:
        """
        Read contents of an S3 object as a string

        :param s3_key: S3 key
        :return: S3 object as a string
        """
        log.debug(f"reading {self.bucket_name}/{s3_key}")
        return self._read_object(s3_key).decode()

    @typechecked()
    def read_lines(self, s3_key: str) -> List[str]:
        """
        Read contents of an S3 object as a list of strings

        :param s3_key: S3 key
        :return: a list of strings
        """
        return self.read_string(s3_key).splitlines()

    @typechecked()
    def write_string(self, input_str: str, s3_key: str):
        """
        Write a string to an S3 object

        :param input_str: input string
        :param s3_key: S3 key
        """
        log.debug(f"writing {self.bucket_name}/{s3_key}")
        input_bytes = input_str.encode()
        self._write_object(input_bytes, s3_key, get_bytes_sha512(input_bytes), get_bytes_crc64nvme(input_bytes), retry=False)

    @typechecked()
    def write_lines(self, input_lines: List[str], s3_key: str):
        """
        Write a list of strings to an S3 bucket

        :param input_lines: a list of  strings
        :param s3_key: S3 key
        """
        self.write_string("\n".join(input_lines), s3_key)

    @typechecked()
    def delete_object(self, s3_key: str):
        """
        Delete an S3 object

        :param s3_key: S3 key
        """
        log.info(f"deleting {self.bucket_name}/{s3_key}")
        assert self.resource is not None
        self.resource.Object(self.bucket_name, s3_key).delete()

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # objects: upload
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @typechecked()
    def upload(self, file_path: Union[str, Path], s3_key: str, force: bool = False) -> bool:
        """
        Upload a file to an S3 object

        :param file_path: path to file to upload
        :param s3_key: S3 key
        :param force: True to force the upload, even if the file hash matches the S3 contents
        :return: True if uploaded, False if the S3 object was already up to date. Raises AWSimpleException if the upload fails after all retries.
        """

        log.info(f'S3 upload : "{file_path}" to {self.bucket_name}/{s3_key}')

        file_path = Path(file_path)
        file_size = os.path.getsize(file_path)
        file_crc64nvme = get_file_crc64nvme(file_path)
        file_sha512 = get_file_sha512(file_path)

        if not (force or self._is_upload_needed(s3_key, file_size, file_crc64nvme, file_sha512, os.path.getmtime(file_path))):
            log.info(f"file checksum of {file_crc64nvme} is the same as is already on S3 and force={force} - not uploading")
            return False

        log.info(f"local file : {file_crc64nvme=},{file_sha512=},force={force} - uploading")
        extra_args = self._upload_extra_args(file_sha512)

        def _upload(_unused_attempt_number: int) -> bool:
            self.client.upload_file(str(file_path), self.bucket_name, s3_key, ExtraArgs=extra_args, Config=self.get_s3_transfer_config())
            return True

        # a RuntimeError is also tried again (see the threading issue in get_s3_transfer_config())
        self._retry(_upload, f"upload {file_path} to {self.bucket_name}/{s3_key}", (*connection_errors, RuntimeError))
        self._check_written_object(s3_key, file_size, file_crc64nvme)
        return True

    @typechecked()
    def upload_object_as_json(self, json_serializable_object: Union[List, Dict], s3_key: str, force=False) -> bool:
        """
        Upload a serializable Python object to an S3 object

        :param json_serializable_object: serializable object
        :param s3_key: S3 key (".json" is added if it does not end with it)
        :param force: True to force the upload, even if the file hash matches the S3 contents
        :return: True if uploaded, False if the S3 object was already up to date. Raises AWSimpleException if the upload fails after all retries.
        """

        s3_key = _get_json_key(s3_key)
        json_as_bytes = serializable_object_to_json_as_bytes(json_serializable_object)
        json_crc64nvme = get_bytes_crc64nvme(json_as_bytes)
        json_sha512 = get_bytes_sha512(json_as_bytes)

        # an S3 object without a native checksum or without a SHA-512 is always written again, so that it gains one
        if not (force or self._is_upload_needed(s3_key, len(json_as_bytes), json_crc64nvme, json_sha512)):
            log.info(f"checksum of {json_crc64nvme} is the same as is already on S3 and force={force} - not uploading")
            return False

        log.info(f"{json_crc64nvme=},force={force} - uploading")
        self._write_object(json_as_bytes, s3_key, json_sha512, json_crc64nvme, retry=True)
        return True

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # objects: download
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    @typechecked()
    def download(self, s3_key: str, dest_path: Union[str, Path], s3_object_metadata: Union[S3ObjectMetadata, None] = None, callback: Union[Callable[[int], None], None] = None) -> bool:
        """
        Download an S3 object

        :param s3_key: S3 key
        :param dest_path: destination file or directory path. If the path is a directory, the file will be downloaded under that directory, at the path s3_key_to_local_path() gives the S3 key.
        :param s3_object_metadata: the S3 object's metadata, if the caller already has it (from get_s3_object_metadata()). It is what the downloaded file's length and CRC64NVME are
          checked against and where its mtime comes from, so awsimple does not get the metadata again (boto3 still makes its own request for the size of the object). If the
          check fails the download is tried again with new metadata, since what the caller had may be out of date.
        :param callback: called as bytes are transferred, with the number of bytes transferred since the last call (may be called from other threads). If the download is tried
          again, the bytes of every attempt are reported.
        :return: True if downloaded successfully. Raises AWSimpleException if the download fails after all retries.
        """

        if isinstance(dest_path, str):
            log.info(f"{dest_path} is not Path object.  Non-Path objects will be deprecated in the future")
            dest_path = Path(dest_path)

        if dest_path.is_dir():
            dest_path = s3_key_to_local_path(dest_path, s3_key)

        log.info(f'S3 download : {self.bucket_name}:{s3_key} to "{dest_path}" ("{dest_path.absolute()}")')

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        known_metadata = s3_object_metadata

        def _download(attempt_number: int) -> bool:
            log.debug(sf("calling client.download_file()", bucket_name=self.bucket_name, s3_key=s3_key, dest_path=dest_path))
            self.client.download_file(self.bucket_name, s3_key, dest_path, Callback=callback, Config=self.get_s3_download_transfer_config())
            # get the metadata if the caller did not give it, or it may be out of date since an attempt with it failed
            metadata = self.get_s3_object_metadata(s3_key) if known_metadata is None or attempt_number > 0 else known_metadata
            log.debug(sf("S3 object metadata", s3_object_metadata=metadata))
            # not intact: an incomplete or corrupted transfer, or the S3 object was written again during the download
            return self._finish_download(dest_path, metadata)

        self._retry(_download, f"download {self.bucket_name}/{s3_key} to {dest_path}")
        return True

    @typechecked()
    def download_cached(self, s3_key: str, dest_path: Path) -> S3DownloadStatus:
        """
        download from AWS S3 with caching

        :param dest_path: destination full path or directory. If the path is a directory, the file will be downloaded under that directory, at the path s3_key_to_local_path() gives the S3 key.
        :param s3_key: S3 key of source
        :return: S3DownloadStatus instance
        """

        if dest_path.is_dir():
            dest_path = s3_key_to_local_path(dest_path, s3_key)
        log.info(f'S3 download_cached : {self.bucket_name}:{s3_key} to "{dest_path}" ("{dest_path.absolute()}")')

        self.download_status = S3DownloadStatus()  # init
        content_cache = self._get_content_cache()
        s3_object_metadata = self.get_s3_object_metadata(s3_key)

        self.download_status.cache_hit = content_cache.copy_to(s3_object_metadata, dest_path)
        if not self.download_status.cache_hit:
            log.info(f"{self.bucket_name=}/{s3_key=} cache miss : {dest_path=} ({dest_path.absolute()})")
            self.download(s3_key, dest_path, s3_object_metadata)  # raises AWSimpleException on failure, so we don't write a bad cache entry or falsely report success
            self.download_status.cache_write = content_cache.write(s3_object_metadata, dest_path, get_file_sha512(dest_path))
        self.download_status.success = True

        return self.download_status

    @typechecked()
    def download_object_as_json(self, s3_key: str) -> Union[List, Dict]:
        """
        Download an S3 object that is JSON as a Python object

        :param s3_key: S3 key (".json" is added if it does not end with it)
        :return: the Python object
        """
        s3_key = _get_json_key(s3_key)
        obj = json.loads(self._read_object(s3_key).decode("utf-8"))
        return obj

    @typechecked()
    def download_object_as_json_cached(self, s3_key: str) -> Union[List, Dict]:
        """
        Download an S3 object that is JSON as a Python object, with caching. What was done is in self.download_status.

        :param s3_key: S3 key (".json" is added if it does not end with it)
        :return: the Python object
        """
        s3_key = _get_json_key(s3_key)

        self.download_status = S3DownloadStatus()  # init
        content_cache = self._get_content_cache()
        s3_object_metadata = self.get_s3_object_metadata(s3_key)

        body = content_cache.read(s3_object_metadata)
        self.download_status.cache_hit = body is not None
        if body is None:
            log.info(f"{self.bucket_name=}/{s3_key=} cache miss)")
            body = self._read_object(s3_key)
            object_from_json = json.loads(body)  # before the cache write, so that contents that are not JSON are not cached
            self.download_status.cache_write = content_cache.write(s3_object_metadata, body, get_bytes_sha512(body))
        else:
            object_from_json = json.loads(body)
        self.download_status.success = True

        return object_from_json

    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # sync
    # ------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def _sync_download_object(self, transfer_manager: TransferManager, sync_object: S3SyncObject, stop_event: Union[Event, None]) -> S3SyncObject:
        """
        Download one S3 object of a sync to its local file, with one metadata request. The download is to a temporary file that only becomes the local file once its
        length and CRC64NVME (if the S3 object has one) are the S3 object's. A failure is put in the sync object, not raised.

        :param transfer_manager: transfer manager to download with
        :param sync_object: the object to download
        :param stop_event: set to request a stop
        :return: the sync object, with the outcome of the download
        """
        local_path = sync_object.local_path
        assert local_path is not None
        assert self.bucket_name is not None
        if stop_event is not None and stop_event.is_set():
            sync_object.set_outcome(S3SyncOutcome.not_attempted)
            return sync_object
        try:
            s3_object_metadata = self.get_s3_object_metadata(sync_object.key)
            sync_object.size = s3_object_metadata.size  # the object may have been written again since the listing
            # on a versioned bucket, download the version the metadata is of
            extra_args = {} if s3_object_metadata.version_id is None else {"VersionId": s3_object_metadata.version_id}
            local_path.parent.mkdir(parents=True, exist_ok=True)
            with atomic_write_path(local_path) as temp_path:
                transfer_manager.download(self.bucket_name, sync_object.key, str(temp_path), extra_args, [ProvideMetadataSubscriber(s3_object_metadata)]).result()
                if not self._finish_download(temp_path, s3_object_metadata):
                    raise AWSimpleException(f"{self.bucket_name}/{sync_object.key} : what was downloaded does not have the length and CRC64NVME of the S3 object")
            sync_object.set_outcome(S3SyncOutcome.downloaded)
        except (AWSimpleException, OSError, RetriesExceededError, S3DownloadFailedError, *connection_errors) as e:
            # a failure of one object is an outcome of a sync, which goes on with the other objects
            log.warning(f'{self.bucket_name}/{sync_object.key} to "{local_path}" : {e}')
            sync_object.set_outcome(S3SyncOutcome.failed, str(e))
        return sync_object

    @typechecked()
    def sync_to_directory(
        self,
        dest_dir: Path,
        prefix: str = "",
        size_only: bool = False,
        dry_run: bool = False,
        stop_event: Union[Event, None] = None,
        callback: Union[Callable[[S3SyncObject], None], None] = None,
    ) -> S3SyncResult:
        """
        Bring a local directory up to date with this S3 bucket (or the objects with a prefix), like "aws s3 sync" from a bucket to a directory. Local files are never deleted.

        The bucket is listed once (see list_objects()). An object is downloaded when its local file is missing, has a different size, or is older than the S3 object. Only an
        object that is downloaded takes a metadata request (one), which the length and CRC64NVME of what was downloaded are checked against. A downloaded file gets the
        modification time of the S3 object, and only replaces the local file once it is complete and checked.

        The local file of a key is s3_key_to_local_path() of the key without the prefix, under dest_dir. Not downloaded, and in the result as skipped with the reason: keys that
        end with "/" (folder markers), objects in a storage class that has to be restored first (GLACIER, DEEP_ARCHIVE - also when they have been restored), every key of a
        group that is the same local file (e.g. "a/b" and "a//b", or on Windows and macOS keys that only differ in case), and a key that is a file where other keys need a
        directory ("a" when there is also "a/b"). A key that can not be a local file, or a download that fails, is in the result as failed with the reason, and the sync goes
        on with the other objects. Raises BucketNotFound if the bucket does not exist.

        Downloads use get_s3_download_transfer_config() (by default 10 at a time).

        :param dest_dir: destination directory (made if it does not exist)
        :param prefix: only sync objects that have this prefix in their keys (omit for all objects). It is a string prefix, so it normally ends with "/".
        :param size_only: True to only compare sizes, not modification times
        :param dry_run: True to download nothing (and make no directory): the objects that a sync downloads are in the result as S3SyncOutcome.would_download
        :param stop_event: set this event (e.g. from the callback or another thread) to request a stop: downloads that have started are completed, and the others are in the
          result as S3SyncOutcome.not_attempted
        :param callback: called with every object when its outcome is known, from the thread that called this method
        :return: the outcome of every listed object. Its get_count() and get_size() without an outcome are the object count and total size of the listing.
        """
        result = S3SyncResult(plan_sync(self.list_objects(prefix), dest_dir, prefix, size_only, self.mtime_abs_tol))
        to_download = result.get_objects(S3SyncOutcome.would_download)
        if callback is not None:
            for sync_object in result.objects.values():
                if dry_run or sync_object.outcome != S3SyncOutcome.would_download:
                    callback(sync_object)
        if dry_run or len(to_download) == 0:
            return result

        log.info(f'S3 sync : {self.bucket_name}/{prefix} to "{dest_dir}" : downloading {len(to_download)} of {len(result.objects)} objects')
        dest_dir.mkdir(parents=True, exist_ok=True)
        transfer_config = self.get_s3_download_transfer_config()
        max_workers = transfer_config.max_request_concurrency if transfer_config.use_threads else 1
        executor_cls = None if transfer_config.use_threads else NonThreadedExecutor
        with TransferManager(self.client, transfer_config, executor_cls=executor_cls) as transfer_manager, ThreadPoolExecutor(max_workers) as executor:
            futures = [executor.submit(self._sync_download_object, transfer_manager, sync_object, stop_event) for sync_object in to_download]
            for future in as_completed(futures):
                if callback is not None:
                    callback(future.result())
        result.stopped = result.get_count(S3SyncOutcome.not_attempted) > 0
        return result
