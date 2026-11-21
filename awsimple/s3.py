"""
S3 Access
"""

import base64
import os
import re
import shutil
import time
from math import isclose
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Union
import json
from logging import getLogger

from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError, SSLError
from boto3.s3.transfer import TransferConfig
from s3transfer import S3UploadFailedError
import urllib3.exceptions
from typeguard import typechecked
from hashy import get_file_sha512, get_bytes_sha512, get_dls_sha512, get_file_crc64nvme, get_bytes_crc64nvme
from yasf import sf

from awsimple import (
    CacheAccess,
    __application_name__,
    lru_cache_write,
    atomic_write_path,
    AWSimpleException,
    convert_serializable_special_cases,
    S3BucketAlreadyExistsNotOwnedByYou,
    is_using_localstack,
    boto_error_to_string,
)

# Use this project's name as a prefix to avoid string collisions.  Use dashes instead of underscore since that's AWS's convention.
# The key of the S3 object metadata that holds the SHA-512 of the object's contents (as a hex string). awsimple writes it on every object it writes, since an S3 object
# has one native checksum and awsimple's is CRC64NVME (see below). S3 does not validate object metadata, so awsimple checks it against the contents it downloads
# before it caches them.
sha512_string = f"{__application_name__}-sha512"
sha512_hex_regex = re.compile(r"[0-9a-f]{128}")

# Uploads at or above this size use multipart (the boto3 default threshold). awsimple uses CRC64NVME checksums everywhere since
# it's the only checksum family S3 computes as a full-object value for both single-part and multipart uploads (SHA-family multipart
# checksums are composite and can't be compared to a local file hash), and it's also S3's own default - so objects written by other
# modern tools are usually comparable too.
default_multipart_threshold = 8 * 1024 * 1024  # 8 MiB

json_extension = ".json"

# Part of every S3 identity hash (see S3ObjectMetadata.get_identity_sha512()). Change it whenever the fields in the identity hash change, so that hashes made the old way are never matched.
cache_key_scheme = f"{__application_name__}-s3-cache-key-2"

# Sub-directory of the S3 cache directory. For an S3 object that has no SHA-512, it holds the SHA-512 of the contents awsimple downloaded, in a file named with the object's identity hash.
cache_index_dir_name = "index"

log = getLogger(__application_name__)

connection_errors = (S3UploadFailedError, ClientError, EndpointConnectionError, SSLError, urllib3.exceptions.ProtocolError, ConnectionClosedError)


class BucketNotFound(AWSimpleException):
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.message = "Bucket not found"
        super().__init__(self.message)

    def __str__(self):
        return f"{self.bucket_name=} {self.message}"


@dataclass
class S3DownloadStatus:
    success: bool = False
    cache_hit: Union[bool, None] = None
    cache_write: Union[bool, None] = None


@dataclass
class S3ObjectMetadata:
    bucket: str
    key: str
    size: int
    mtime: datetime
    etag: str  # generally not used
    sha512: Union[str, None]  # hex string - S3's native full-object SHA-512 checksum (None if the object doesn't have one, e.g. multipart uploads or objects written by other tools)
    url: str  # URL of S3 object
    legacy_sha512: Union[str, None] = (
        None  # hex string - SHA-512 from awsimple's object metadata, as written (S3 does not validate it). The name is from when only older awsimple versions wrote it.
    )
    crc64nvme: Union[str, None] = None  # hex string - S3's native full-object CRC64NVME checksum (awsimple uses this for multipart uploads)
    version_id: Union[str, None] = None  # S3 version ID (None if the bucket is not versioned)
    checksum_type: Union[str, None] = None  # S3's native checksum type: "FULL_OBJECT" or "COMPOSITE" (None if the object has no native checksum)
    checksums: Dict[str, str] = field(default_factory=dict)  # every native checksum S3 returned, as returned (base64, composite included), by response field name, e.g. "ChecksumCRC64NVME"

    def get_content_sha512(self) -> Union[str, None]:
        """
        Get the SHA-512 of this object's contents from S3's metadata: S3's native full-object SHA-512 if the object has one, otherwise the SHA-512 in awsimple's object metadata.

        :return: SHA-512 (as hex string), or None if the object has neither (or the object metadata value is not a SHA-512)
        """
        if self.sha512 is not None:
            return self.sha512
        if self.legacy_sha512 is not None and sha512_hex_regex.fullmatch(self.legacy_sha512) is not None:
            return self.legacy_sha512
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


@typechecked()
def serializable_object_to_json_as_bytes(json_serializable_object: Union[List, Dict]) -> bytes:
    return bytes(json.dumps(json_serializable_object, default=convert_serializable_special_cases).encode("UTF-8"))


@typechecked()
def _native_checksum_to_hex(checksum_base64: Union[str, None]) -> Union[str, None]:
    """
    Convert an S3 native checksum (base64 encoded) to a hex string comparable with locally computed hashes.
    Composite (multipart) checksums have a "-<part count>" suffix and do not represent the whole object, so they convert to None.

    :param checksum_base64: base64 encoded checksum from S3 (or None)
    :return: hex string, or None if no full-object checksum available
    """
    if checksum_base64 is None or "-" in checksum_base64:
        return None
    return base64.b64decode(checksum_base64).hex()


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
        # workaround threading issue https://github.com/boto/s3transfer/issues/197
        # derived class can overload this if a different config is desired (the multipart threshold also determines which native
        # checksum algorithm uploads use - see _upload_extra_args)
        s3_transfer_config = TransferConfig(use_threads=False, multipart_threshold=default_multipart_threshold)
        return s3_transfer_config

    @typechecked()
    def set_public_readable(self, public_readable: bool):
        self.public_readable = public_readable

    @typechecked()
    def bucket_list(self) -> list:
        """
        list out all buckets
        (not called list_buckets() since that's used in boto3 but this returns a list of bucket strings not a list of dicts)

        :return: list of buckets
        """
        return [b["Name"] for b in self.client.list_buckets()["Buckets"]]

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
        s3_crc64nvme = _native_checksum_to_hex(response.get("ChecksumCRC64NVME"))
        if s3_crc64nvme is not None and (body_crc64nvme := get_bytes_crc64nvme(body)) != s3_crc64nvme:
            raise AWSimpleException(f"{self.bucket_name}/{s3_key} : the CRC64NVME of what was read is {body_crc64nvme} but the S3 object's is {s3_crc64nvme}")
        return body

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
        assert self.resource is not None
        input_bytes = input_str.encode()
        self.resource.Object(self.bucket_name, s3_key).put(Body=input_bytes, **self._upload_extra_args(get_bytes_sha512(input_bytes)))
        self._check_written_object(s3_key, len(input_bytes), get_bytes_crc64nvme(input_bytes))

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

    def _is_upload_needed(self, file_path: Path, s3_key: str, file_crc64nvme: str, file_sha512: str) -> bool:
        """
        Determine if a file differs from the S3 object it would be uploaded to.

        :param file_path: path to file to upload
        :param s3_key: S3 key
        :param file_crc64nvme: CRC64NVME checksum of the file
        :param file_sha512: SHA-512 of the file
        :return: True if the S3 object does not exist or does not have the file's contents
        """
        if not self.object_exists(s3_key):
            return True

        s3_object_metadata = self.get_s3_object_metadata(s3_key)
        log.info(f"{s3_object_metadata=}")
        content_sha512 = s3_object_metadata.get_content_sha512()
        if s3_object_metadata.crc64nvme is not None:
            # use the native full-object checksum, if the object has one. A CRC is not collision resistant, so the size and the SHA-512 (if the object has one) must also be the same.
            crcs_equal = file_crc64nvme == s3_object_metadata.crc64nvme
            sizes_equal = os.path.getsize(file_path) == s3_object_metadata.size
            sha512s_equal = content_sha512 is None or content_sha512 == file_sha512
            upload_flag = not (crcs_equal and sizes_equal and sha512s_equal)
        elif s3_object_metadata.sha512 is not None:
            # the object was written by another tool with a native full-object SHA-512 checksum
            upload_flag = file_sha512 != s3_object_metadata.sha512
        elif s3_object_metadata.legacy_sha512 is not None:
            # the object has the metadata SHA-512 but no native checksum (e.g. written by an older awsimple) - always re-upload it so it gains the native checksum
            upload_flag = True
        else:
            # no hash is available, so compare modification time and file size (free and robust, but weaker than a hash)
            sizes_equal = os.path.getsize(file_path) == s3_object_metadata.size
            mtimes_equal = isclose(os.path.getmtime(file_path), s3_object_metadata.mtime.timestamp(), abs_tol=self.mtime_abs_tol)
            upload_flag = not (sizes_equal and mtimes_equal)
        return upload_flag

    def _upload_file_with_retries(self, file_path: Path, s3_key: str, file_sha512: str) -> bool:
        """
        Upload a file to an S3 object, trying again after an error.

        :param file_path: path to file to upload
        :param s3_key: S3 key
        :param file_sha512: SHA-512 of the file
        :return: True if uploaded, False if every attempt failed
        """
        uploaded_flag = False
        transfer_retry_count = 0
        extra_args = self._upload_extra_args(file_sha512)
        log.info(f"{extra_args=}")
        while not uploaded_flag and transfer_retry_count < self.retry_count:
            try:
                self.client.upload_file(str(file_path), self.bucket_name, s3_key, ExtraArgs=extra_args, Config=self.get_s3_transfer_config())
                uploaded_flag = True
            except connection_errors as e:
                log.warning(f"{file_path} to {self.bucket_name}:{s3_key} : {transfer_retry_count=} : {e}")
                time.sleep(self.retry_sleep_time)
            except RuntimeError as e:
                log.error(f"{file_path} to {self.bucket_name}:{s3_key} : {transfer_retry_count=} : {e}")
                time.sleep(self.retry_sleep_time)

            transfer_retry_count += 1
        return uploaded_flag

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

        if isinstance(file_path, str):
            file_path = Path(file_path)

        file_crc64nvme = get_file_crc64nvme(file_path)
        file_sha512 = get_file_sha512(file_path)

        uploaded_flag = False
        if force or self._is_upload_needed(file_path, s3_key, file_crc64nvme, file_sha512):
            log.info(f"local file : {file_crc64nvme=},{file_sha512=},force={force} - uploading")

            uploaded_flag = self._upload_file_with_retries(file_path, s3_key, file_sha512)
            if not uploaded_flag:
                raise AWSimpleException(f"couldn't upload {file_path} to {self.bucket_name}/{s3_key} after {self.retry_count} attempts")
            self._check_written_object(s3_key, os.path.getsize(file_path), file_crc64nvme)

        else:
            log.info(f"file checksum of {file_crc64nvme} is the same as is already on S3 and force={force} - not uploading")

        return uploaded_flag

    @typechecked()
    def upload_object_as_json(self, json_serializable_object: Union[List, Dict], s3_key: str, force=False) -> bool:
        """
        Upload a serializable Python object to an S3 object

        :param json_serializable_object: serializable object
        :param s3_key: S3 key
        :param force: True to force the upload, even if the file hash matches the S3 contents
        :return: True if uploaded
        """

        s3_key = _get_json_key(s3_key)
        json_as_bytes = serializable_object_to_json_as_bytes(json_serializable_object)
        json_crc64nvme = get_bytes_crc64nvme(json_as_bytes)
        json_sha512 = get_bytes_sha512(json_as_bytes)
        upload_flag = True
        if not force and self.object_exists(s3_key):
            s3_object_metadata = self.get_s3_object_metadata(s3_key)
            log.info(f"{s3_object_metadata=}")
            content_sha512 = s3_object_metadata.get_content_sha512()
            if s3_object_metadata.crc64nvme is not None:
                # use the native full-object checksum, if the object has one. A CRC is not collision resistant, so the size and the SHA-512 (if the object has one) must also be the same.
                crcs_equal = json_crc64nvme == s3_object_metadata.crc64nvme
                sizes_equal = len(json_as_bytes) == s3_object_metadata.size
                sha512s_equal = content_sha512 is None or content_sha512 == json_sha512
                upload_flag = not (crcs_equal and sizes_equal and sha512s_equal)
            elif s3_object_metadata.sha512 is not None:
                # the object was written by another tool with a native full-object SHA-512 checksum
                upload_flag = json_sha512 != s3_object_metadata.sha512
            # otherwise upload_flag stays True - objects without a native checksum are always re-uploaded so they gain it

        uploaded_flag = False
        if upload_flag:
            log.info(f"{json_crc64nvme=},force={force} - uploading")

            transfer_retry_count = 0
            while not uploaded_flag and transfer_retry_count < self.retry_count:
                assert self.resource is not None
                try:
                    s3_object = self.resource.Object(self.bucket_name, s3_key)
                    s3_object.put(Body=json_as_bytes, **self._upload_extra_args(json_sha512))
                    uploaded_flag = True
                except connection_errors as e:
                    log.warning(f"{self.bucket_name}:{s3_key} : {transfer_retry_count=} : {e}")
                    transfer_retry_count += 1
                    time.sleep(self.retry_sleep_time)

            if not uploaded_flag:
                raise AWSimpleException(f"couldn't upload JSON to {self.bucket_name}/{s3_key} after {self.retry_count} attempts")
            self._check_written_object(s3_key, len(json_as_bytes), json_crc64nvme)

        else:
            log.info(f"checksum of {json_crc64nvme} is the same as is already on S3 and force={force} - not uploading")

        return uploaded_flag

    @typechecked()
    def download(self, s3_key: str, dest_path: Union[str, Path]) -> bool:
        """
        Download an S3 object

        :param s3_key: S3 key
        :param dest_path: destination file or directory path. If the path is a directory, the file will be downloaded to that directory with the same name as the S3 key.
        :return: True if downloaded successfully. Raises AWSimpleException if the download fails after all retries.
        """

        if isinstance(dest_path, str):
            log.info(f"{dest_path} is not Path object.  Non-Path objects will be deprecated in the future")
            dest_path = Path(dest_path)

        if dest_path.is_dir():
            dest_path = Path(dest_path, s3_key)

        log.info(f'S3 download : {self.bucket_name}:{s3_key} to "{dest_path}" ("{Path(dest_path).absolute()}")')

        Path(dest_path).parent.mkdir(parents=True, exist_ok=True)

        transfer_retry_count = 0
        success = False
        while not success and transfer_retry_count < self.retry_count:
            try:
                log.debug(sf("calling client.download_file()", bucket_name=self.bucket_name, s3_key=s3_key, dest_path=dest_path))
                self.client.download_file(self.bucket_name, s3_key, dest_path)
                log.debug(sf("S3 client.download_file() complete", bucket_name=self.bucket_name, s3_key=s3_key, dest_path=dest_path))
                s3_object_metadata = self.get_s3_object_metadata(s3_key)
                log.debug(sf("S3 object metadata", s3_object_metadata=s3_object_metadata))
                if self._is_download_intact(Path(dest_path), s3_object_metadata):
                    mtime_ts = s3_object_metadata.mtime.timestamp()
                    os.utime(dest_path, (mtime_ts, mtime_ts))  # set the file mtime to the mtime in S3
                    success = True
                else:
                    # an incomplete or corrupted transfer, or the S3 object was written again during the download - try again
                    time.sleep(self.retry_sleep_time)
                    transfer_retry_count += 1
            except connection_errors as e:
                # ProtocolError can happen for a broken connection
                log.warning(f"{self.bucket_name}/{s3_key} to {dest_path} ({Path(dest_path).absolute()}) : {transfer_retry_count=} : {e}")
                time.sleep(self.retry_sleep_time)
                transfer_retry_count += 1
        log.debug(sf(transfer_retry_count=transfer_retry_count, success=success, bucket_name=self.bucket_name, s3_key=s3_key, dest_path=dest_path))
        if not success:
            raise AWSimpleException(f"couldn't download {self.bucket_name}/{s3_key} to {dest_path} after {self.retry_count} attempts")
        return success

    @typechecked()
    def download_cached(self, s3_key: str, dest_path: Path) -> S3DownloadStatus:
        """
        download from AWS S3 with caching

        :param dest_path: destination full path or directory. If the path is a directory, the file will be downloaded to that directory with the same name as the S3 key.
        :param s3_key: S3 key of source
        :return: S3DownloadStatus instance
        """

        if dest_path.is_dir():
            dest_path = Path(dest_path, s3_key)
        log.info(f'S3 download_cached : {self.bucket_name}:{s3_key} to "{dest_path}" ("{dest_path.absolute()}")')

        self.download_status = S3DownloadStatus()  # init

        s3_object_metadata = self.get_s3_object_metadata(s3_key)

        if (cache_path := self._get_cached_path(s3_object_metadata)) is None:
            self.download_status.cache_hit = False
            log.info(f"{self.bucket_name=}/{s3_key=} cache miss : {dest_path=} ({dest_path.absolute()})")
            self.download(s3_key, dest_path)  # raises AWSimpleException on failure, so we don't write a bad cache entry or falsely report success
            self.download_status.cache_write = self._write_cache(s3_object_metadata, dest_path, get_file_sha512(dest_path))
        else:
            log.info(f"{self.bucket_name}/{s3_key} cache hit : copying {cache_path=} to {dest_path=} ({dest_path.absolute()})")
            self.download_status.cache_hit = True
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(cache_path, dest_path)
            # the cached contents may have been downloaded from another S3 object with the same contents, so set the file mtime to the mtime of this S3 object (as a download does)
            mtime_ts = s3_object_metadata.mtime.timestamp()
            os.utime(dest_path, (mtime_ts, mtime_ts))
        self.download_status.success = True

        return self.download_status

    def _get_cached_path(self, s3_object_metadata: S3ObjectMetadata) -> Union[Path, None]:
        """
        Find an S3 object's contents in the cache. Cached contents are in a file named with the SHA-512 of the contents.

        :param s3_object_metadata: S3 object metadata
        :return: path to the cached contents, or None if they are not in the cache
        """
        if (content_sha512 := s3_object_metadata.get_content_sha512()) is None:
            # the S3 object has no SHA-512, so initially use its identity hash to look up the SHA-512 of the contents awsimple downloaded
            index_path = Path(self.cache_dir, cache_index_dir_name, s3_object_metadata.get_identity_sha512())
            try:
                content_sha512 = index_path.read_text().strip()
            except FileNotFoundError:
                return None
            if sha512_hex_regex.fullmatch(content_sha512) is None:
                return None
        cache_path = Path(self.cache_dir, content_sha512)
        log.debug(f"{cache_path}")
        # the length is checked so that an incomplete cache file is not used
        if cache_path.is_file() and os.path.getsize(cache_path) == s3_object_metadata.size:
            return cache_path
        return None

    def _write_cache(self, s3_object_metadata: S3ObjectMetadata, new_data: Union[Path, bytes], new_data_sha512: str) -> bool:
        """
        Write the contents of an S3 object that were just downloaded to the cache, in a file named with the SHA-512 of the contents. Raises AWSimpleException if the
        S3 object's SHA-512 is not the SHA-512 of the contents.

        :param s3_object_metadata: S3 object metadata from before the download
        :param new_data: the downloaded contents (a file path or bytes)
        :param new_data_sha512: SHA-512 calculated from the downloaded contents
        :return: True if written to the cache
        """
        if (content_sha512 := s3_object_metadata.get_content_sha512()) is None:
            index_path = Path(self.cache_dir, cache_index_dir_name, s3_object_metadata.get_identity_sha512())
            index_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with atomic_write_path(index_path) as temp_path:
                    temp_path.write_text(new_data_sha512)
            except PermissionError as e:
                # e.g. on Windows, another process has the index file open. The contents are not cached this time, which is a normal outcome of a cache write.
                log.info(f"{index_path=} {e}")
                return False
        elif content_sha512 != new_data_sha512:
            raise AWSimpleException(
                f"{self.bucket_name}/{s3_object_metadata.key} : the SHA-512 of the downloaded contents is {new_data_sha512} but the S3 object's SHA-512 is {content_sha512} "
                "(the S3 object was written again during the download, or the S3 object's metadata is wrong)"
            )
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        return lru_cache_write(new_data, self.cache_dir, new_data_sha512, self.cache_max_absolute, self.cache_max_of_free)

    @typechecked()
    def download_object_as_json(self, s3_key: str) -> Union[List, Dict]:
        s3_key = _get_json_key(s3_key)
        obj = json.loads(self._read_object(s3_key).decode("utf-8"))
        return obj

    @typechecked()
    def download_object_as_json_cached(self, s3_key: str) -> Union[List, Dict]:
        """
        download object from AWS S3 with caching

        :param s3_key: S3 key of source
        :return: S3DownloadStatus instance
        """
        object_from_json = None

        s3_key = _get_json_key(s3_key)

        self.download_status = S3DownloadStatus()  # init

        s3_object_metadata = self.get_s3_object_metadata(s3_key)

        if (cache_path := self._get_cached_path(s3_object_metadata)) is None:
            log.info(f"{self.bucket_name=}/{s3_key=} cache miss)")
            self.download_status.cache_hit = False
            body = self._read_object(s3_key)
            object_from_json = json.loads(body)
            self.download_status.cache_write = self._write_cache(s3_object_metadata, body, get_bytes_sha512(body))
        else:
            log.info(f"{self.bucket_name}/{s3_key} cache hit : using {cache_path=}")
            self.download_status.cache_hit = True
            object_from_json = json.loads(cache_path.read_bytes())
        self.download_status.success = True

        if object_from_json is None:
            raise RuntimeError(s3_key)

        return object_from_json

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
        assert isinstance(self.bucket_name, str)  # mainly for mypy
        # sha512 and crc64nvme are S3's native (server-validated) full-object checksums; legacy_sha512 is the SHA-512 in awsimple's object metadata (not validated by S3)
        s3_object_metadata = S3ObjectMetadata(
            self.bucket_name,
            s3_key,
            head["ContentLength"],
            head["LastModified"],
            head["ETag"][1:-1].lower(),
            _native_checksum_to_hex(head.get("ChecksumSHA512")),
            self.get_s3_object_url(s3_key),
            head.get("Metadata", {}).get(sha512_string),
            _native_checksum_to_hex(head.get("ChecksumCRC64NVME")),
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
    def bucket_exists(self) -> bool:
        """
        Test if S3 bucket exists

        :return: True if bucket exists
        """

        # use a "custom" config so that .head_bucket() doesn't take a really long time if the bucket does not exist
        if self.is_mocked() or is_using_localstack():
            s3 = self.client  # the existing client is already pointed at the mock or localstack endpoint
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

    @typechecked()
    def dir(self, prefix: str = "") -> Dict[str, S3ObjectMetadata]:
        """
        Do a "directory" of an S3 bucket where the returned dict key is the S3 key and the value is an S3ObjectMetadata object.

        Use the faster .keys() method if all you need are the keys.

        :param prefix: only do a dir on objects that have this prefix in their keys (omit for all objects)
        :return: a dict where key is the S3 key and the value is S3ObjectMetadata
        """
        directory = {}
        if self.bucket_exists():
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                # deal with empty bucket
                for content in page.get("Contents", []):
                    s3_key = content.get("Key")
                    directory[s3_key] = self.get_s3_object_metadata(s3_key)
        else:
            raise BucketNotFound(self.bucket_name)
        return directory

    def keys(self, prefix: str = "") -> List[str]:
        """
        List all the keys in this S3 Bucket.

        Note that this should be faster than .dir() if all you need are the keys and not the metadata.

        :param prefix: only do a dir on objects that have this prefix in their keys (omit for all objects)
        :return: a sorted list of all the keys in this S3 Bucket (sorted for consistency)
        """
        keys = []
        if self.bucket_exists():
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                # deal with empty bucket
                for content in page.get("Contents", []):
                    s3_key = content.get("Key")
                    keys.append(s3_key)
        else:
            raise BucketNotFound(self.bucket_name)
        keys.sort()
        return keys
