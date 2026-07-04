"""
S3 Access
"""

import base64
import os
import shutil
import time
from math import isclose
from pathlib import Path
from dataclasses import dataclass
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
from hashy import get_file_sha512, get_bytes_sha512, get_dls_sha512
from yasf import sf

from awsimple import (
    CacheAccess,
    __application_name__,
    lru_cache_write,
    AWSimpleException,
    convert_serializable_special_cases,
    S3BucketAlreadyExistsNotOwnedByYou,
    is_using_localstack,
    boto_error_to_string,
)

# Use this project's name as a prefix to avoid string collisions.  Use dashes instead of underscore since that's AWS's convention.
# This custom metadata predates S3's native SHA-512 checksum support (April 2026). It is no longer written - it is only read to
# detect objects written by older awsimple versions, which are always re-uploaded so that every object gains the native checksum.
sha512_string = f"{__application_name__}-sha512"

# Uploads at or above this size use multipart (the boto3 default threshold). awsimple uses CRC64NVME checksums everywhere since
# it's the only checksum family S3 computes as a full-object value for both single-part and multipart uploads (SHA-family multipart
# checksums are composite and can't be compared to a local file hash), and it's also S3's own default - so objects written by other
# modern tools are usually comparable too.
default_multipart_threshold = 8 * 1024 * 1024  # 8 MiB

json_extension = ".json"

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
    legacy_sha512: Union[str, None] = None  # hex string from awsimple's legacy custom metadata - only present on objects written by awsimple versions before native checksum support
    crc64nvme: Union[str, None] = None  # hex string - S3's native full-object CRC64NVME checksum (awsimple uses this for multipart uploads)

    def get_sha512(self) -> str:
        """
        Get a content-derived value used to compare and cache S3 objects. If the native SHA512 is available (recommended), then use that. Otherwise use the native full-object
        CRC64NVME (e.g. multipart uploads). If neither is available (e.g. an S3 object wasn't written with AWSimple), create a "substitute" hash that should change if the object
        contents change.
        :return: hash (as string)
        """
        if (sha512_value := self.sha512) is None:
            if (sha512_value := self.crc64nvme) is None:
                # round timestamp to seconds to try to avoid possible small deltas when dealing with time and floats
                mtime_as_int = int(round(self.mtime.timestamp()))
                metadata_list = [self.bucket, self.key, self.size, mtime_as_int]
                if self.etag is not None and len(self.etag) > 0:
                    metadata_list.append(self.etag)
                sha512_value = get_dls_sha512(metadata_list)

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


@typechecked()
def get_bytes_crc64nvme(data: bytes) -> str:
    """
    Compute the CRC64NVME checksum of bytes as a hex string (comparable with S3's native full-object CRC64NVME checksum).

    :param data: input bytes
    :return: CRC64NVME as a hex string
    """
    from awscrt import checksums as awscrt_checksums  # awscrt comes in via boto3[crt] (also required by botocore to compute CRC64NVME checksums on upload)

    return awscrt_checksums.crc64nvme(data, 0).to_bytes(8, "big").hex()


@typechecked()
def get_file_crc64nvme(file_path: Union[str, Path]) -> str:
    """
    Compute the CRC64NVME checksum of a file as a hex string (comparable with S3's native full-object CRC64NVME checksum).

    :param file_path: path to the file
    :return: CRC64NVME as a hex string
    """
    from awscrt import checksums as awscrt_checksums  # awscrt comes in via boto3[crt] (also required by botocore to compute CRC64NVME checksums on upload)

    crc = 0
    with open(file_path, "rb") as f:
        while chunk := f.read(1 << 20):
            crc = awscrt_checksums.crc64nvme(chunk, crc)
    return crc.to_bytes(8, "big").hex()


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

    def _upload_extra_args(self) -> Dict[str, Any]:
        """
        ExtraArgs for uploads: the native full-object CRC64NVME checksum and an optional public-read ACL.

        :return: ExtraArgs dict
        """
        # S3 validates the data against the (SDK computed) checksum before storing
        extra_args: Dict[str, Any] = {"ChecksumAlgorithm": "CRC64NVME"}
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

    @typechecked()
    def read_string(self, s3_key: str) -> str:
        """
        Read contents of an S3 object as a string

        :param s3_key: S3 key
        :return: S3 object as a string
        """
        log.debug(f"reading {self.bucket_name}/{s3_key}")
        assert self.resource is not None
        return self.resource.Object(self.bucket_name, s3_key).get()["Body"].read().decode()

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
        # S3 validates the data against the (SDK computed) checksum before storing
        self.resource.Object(self.bucket_name, s3_key).put(Body=input_str, ChecksumAlgorithm="CRC64NVME")

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

        file_mtime = os.path.getmtime(file_path)
        file_crc64nvme = get_file_crc64nvme(file_path)
        if force:
            upload_flag = True
        else:
            if self.object_exists(s3_key):
                s3_object_metadata = self.get_s3_object_metadata(s3_key)
                log.info(f"{s3_object_metadata=}")
                if s3_object_metadata.crc64nvme is not None:
                    # use the native full-object checksum, if the object has one (note that .get_sha512() never returns None - it synthesizes a substitute hash - so check the field itself)
                    upload_flag = file_crc64nvme != s3_object_metadata.crc64nvme
                elif s3_object_metadata.sha512 is not None:
                    # the object was written by another tool with a native full-object SHA-512 checksum
                    upload_flag = get_file_sha512(file_path) != s3_object_metadata.sha512
                elif s3_object_metadata.legacy_sha512 is not None:
                    # the object was written by an older awsimple using the legacy metadata hash - always re-upload it so it gains the native checksum
                    upload_flag = True
                else:
                    # no hash is available, so compare modification time and file size (free and robust, but weaker than a hash)
                    sizes_equal = os.path.getsize(file_path) == s3_object_metadata.size
                    mtimes_equal = isclose(file_mtime, s3_object_metadata.mtime.timestamp(), abs_tol=self.mtime_abs_tol)
                    upload_flag = not (sizes_equal and mtimes_equal)
            else:
                upload_flag = True

        uploaded_flag = False
        if upload_flag:
            log.info(f"local file : {file_crc64nvme=},force={force} - uploading")

            transfer_retry_count = 0
            extra_args = self._upload_extra_args()
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

            if not uploaded_flag:
                raise AWSimpleException(f"couldn't upload {file_path} to {self.bucket_name}/{s3_key} after {self.retry_count} attempts")

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
        upload_flag = True
        if not force and self.object_exists(s3_key):
            s3_object_metadata = self.get_s3_object_metadata(s3_key)
            log.info(f"{s3_object_metadata=}")
            if s3_object_metadata.crc64nvme is not None:
                # use the native full-object checksum, if the object has one (note that .get_sha512() never returns None - it synthesizes a substitute hash - so check the field itself)
                upload_flag = json_crc64nvme != s3_object_metadata.crc64nvme
            elif s3_object_metadata.sha512 is not None:
                # the object was written by another tool with a native full-object SHA-512 checksum
                upload_flag = get_bytes_sha512(json_as_bytes) != s3_object_metadata.sha512
            # otherwise upload_flag stays True - objects written by older awsimple (legacy metadata hash) or without any hash are always re-uploaded so they gain the native checksum

        uploaded_flag = False
        if upload_flag:
            log.info(f"{json_crc64nvme=},force={force} - uploading")

            transfer_retry_count = 0
            while not uploaded_flag and transfer_retry_count < self.retry_count:
                assert self.resource is not None
                try:
                    s3_object = self.resource.Object(self.bucket_name, s3_key)
                    # S3 validates the data against the (SDK computed) checksum before storing
                    put_kwargs: Dict[str, Any] = {"Body": json_as_bytes, "ChecksumAlgorithm": "CRC64NVME"}
                    if self.public_readable:
                        put_kwargs["ACL"] = "public-read"
                    s3_object.put(**put_kwargs)
                    uploaded_flag = True
                except connection_errors as e:
                    log.warning(f"{self.bucket_name}:{s3_key} : {transfer_retry_count=} : {e}")
                    transfer_retry_count += 1
                    time.sleep(self.retry_sleep_time)

            if not uploaded_flag:
                raise AWSimpleException(f"couldn't upload JSON to {self.bucket_name}/{s3_key} after {self.retry_count} attempts")

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
                mtime_ts = s3_object_metadata.mtime.timestamp()
                os.utime(dest_path, (mtime_ts, mtime_ts))  # set the file mtime to the mtime in S3
                success = True
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

        sha512 = s3_object_metadata.get_sha512()
        cache_path = Path(self.cache_dir, sha512)
        log.debug(f"{cache_path}")

        if cache_path.exists():
            log.info(f"{self.bucket_name}/{s3_key} cache hit : copying {cache_path=} to {dest_path=} ({dest_path.absolute()})")
            self.download_status.cache_hit = True
            self.download_status.success = True
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(cache_path, dest_path)
        else:
            self.download_status.cache_hit = False

        if not self.download_status.cache_hit:
            log.info(f"{self.bucket_name=}/{s3_key=} cache miss : {dest_path=} ({dest_path.absolute()})")
            self.download(s3_key, dest_path)  # raises AWSimpleException on failure, so we don't write a bad cache entry or falsely report success
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self.download_status.cache_write = lru_cache_write(dest_path, self.cache_dir, sha512, self.cache_max_absolute, self.cache_max_of_free)
            self.download_status.success = True

        return self.download_status

    @typechecked()
    def download_object_as_json(self, s3_key: str) -> Union[List, Dict]:
        s3_key = _get_json_key(s3_key)
        assert self.resource is not None
        s3_object = self.resource.Object(self.bucket_name, s3_key)
        body = s3_object.get()["Body"].read().decode("utf-8")
        obj = json.loads(body)
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

        sha512 = s3_object_metadata.get_sha512()
        cache_path = Path(self.cache_dir, sha512)
        log.debug(f"{cache_path}")

        if cache_path.exists():
            log.info(f"{self.bucket_name}/{s3_key} cache hit : using {cache_path=}")
            self.download_status.cache_hit = True
            self.download_status.success = True
            with cache_path.open("rb") as f:
                object_from_json = json.loads(f.read())
        else:
            self.download_status.cache_hit = False

        if not self.download_status.cache_hit:
            log.info(f"{self.bucket_name=}/{s3_key=} cache miss)")
            assert self.resource is not None
            s3_object = self.resource.Object(self.bucket_name, s3_key)
            body = s3_object.get()["Body"].read()
            object_from_json = json.loads(body)
            self.download_status.cache_write = lru_cache_write(body, self.cache_dir, sha512, self.cache_max_absolute, self.cache_max_of_free)
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
        # sha512 and crc64nvme are S3's native (server-validated) full-object checksums; legacy_sha512 is awsimple's legacy custom
        # metadata, kept readable so objects written by older awsimple versions can be detected (and re-uploaded to gain the native checksum)
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
