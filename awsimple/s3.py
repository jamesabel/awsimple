"""
S3 Access
"""

import os
import shutil
import time
from math import isclose
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Union
import json
from logging import getLogger

from botocore.exceptions import ClientError, EndpointConnectionError, ConnectionClosedError, SSLError
from boto3.s3.transfer import TransferConfig
from s3transfer import S3UploadFailedError
import urllib3
import urllib3.exceptions
from typeguard import typechecked
from hashy import get_string_sha512, get_file_sha512, get_bytes_sha512, get_dls_sha512  # type: ignore
from yasf import sf

from awsimple import CacheAccess, __application_name__, lru_cache_write, AWSimpleException, convert_serializable_special_cases

# Use this project's name as a prefix to avoid string collisions.  Use dashes instead of underscore since that's AWS's convention.
sha512_string = f"{__application_name__}-sha512"

json_extension = ".json"

log = getLogger(__application_name__)


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
    sha512: Union[str, None]  # hex string - only entries written with awsimple will have this
    url: str  # URL of S3 object

    def get_sha512(self) -> str:
        """
        Get hash used to compare S3 objects. If the SHA512 is available (recommended), then use that. If not (e.g. an S3 object wasn't written with AWSimple), create a "substitute"
        SHA512 hash that should change if the object contents change.
        :return: SHA512 hash (as string)
        """
        if (sha512_value := self.sha512) is None:
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
    def __init__(self, bucket_name: str = None, **kwargs):
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
        super().__init__(resource_name="s3", **kwargs)

    def get_s3_transfer_config(self) -> TransferConfig:
        # workaround threading issue https://github.com/boto/s3transfer/issues/197
        # derived class can overload this if a different config is desired
        s3_transfer_config = TransferConfig(use_threads=False)
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
        self.resource.Object(self.bucket_name, s3_key).put(Body=input_str, Metadata={sha512_string: get_string_sha512(input_str)})

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
        :return: True if uploaded
        """

        log.info(f'S3 upload : "{file_path}" to {self.bucket_name}/{s3_key}')

        if isinstance(file_path, str):
            file_path = Path(file_path)

        file_mtime = os.path.getmtime(file_path)
        file_sha512 = get_file_sha512(file_path)
        if force:
            upload_flag = True
        else:
            if self.object_exists(s3_key):
                s3_object_metadata = self.get_s3_object_metadata(s3_key)
                log.info(f"{s3_object_metadata=}")
                if s3_object_metadata.get_sha512() is not None and file_sha512 is not None:
                    # use the hash provided by awsimple, if it exists
                    upload_flag = file_sha512 != s3_object_metadata.get_sha512()
                else:
                    # if not, use mtime
                    upload_flag = not isclose(file_mtime, s3_object_metadata.mtime.timestamp(), abs_tol=self.mtime_abs_tol)
            else:
                upload_flag = True

        uploaded_flag = False
        if upload_flag:
            log.info(f"local file : {file_sha512=},force={force} - uploading")

            transfer_retry_count = 0
            while not uploaded_flag and transfer_retry_count < self.retry_count:
                extra_args = {"Metadata": {sha512_string: file_sha512}}
                if self.public_readable:
                    extra_args["ACL"] = "public-read"  # type: ignore
                log.info(f"{extra_args=}")

                try:
                    self.client.upload_file(str(file_path), self.bucket_name, s3_key, ExtraArgs=extra_args, Config=self.get_s3_transfer_config())
                    uploaded_flag = True
                except (S3UploadFailedError, ClientError, EndpointConnectionError, SSLError, urllib3.exceptions.ProtocolError) as e:
                    log.warning(f"{file_path} to {self.bucket_name}:{s3_key} : {transfer_retry_count=} : {e}")
                    time.sleep(self.retry_sleep_time)
                except RuntimeError as e:
                    log.error(f"{file_path} to {self.bucket_name}:{s3_key} : {transfer_retry_count=} : {e}")
                    time.sleep(self.retry_sleep_time)

                transfer_retry_count += 1

        else:
            log.info(f"file hash of {file_sha512} is the same as is already on S3 and force={force} - not uploading")

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
        json_sha512 = get_bytes_sha512(json_as_bytes)
        upload_flag = True
        if not force and self.object_exists(s3_key):
            s3_object_metadata = self.get_s3_object_metadata(s3_key)
            log.info(f"{s3_object_metadata=}")
            if s3_object_metadata.get_sha512() is not None and json_sha512 is not None:
                # use the hash provided by awsimple, if it exists
                upload_flag = json_sha512 != s3_object_metadata.get_sha512()

        uploaded_flag = False
        if upload_flag:
            log.info(f"{json_sha512=},force={force} - uploading")

            transfer_retry_count = 0
            while not uploaded_flag and transfer_retry_count < self.retry_count:
                meta_data = {sha512_string: json_sha512}
                log.info(f"{meta_data=}")
                assert self.resource is not None
                try:
                    s3_object = self.resource.Object(self.bucket_name, s3_key)
                    if self.public_readable:
                        s3_object.put(Body=json_as_bytes, Metadata=meta_data, ACL="public-read")
                    else:
                        s3_object.put(Body=json_as_bytes, Metadata=meta_data)
                    uploaded_flag = True
                except (S3UploadFailedError, ClientError, EndpointConnectionError, SSLError, urllib3.exceptions.ProtocolError) as e:
                    log.warning(f"{self.bucket_name}:{s3_key} : {transfer_retry_count=} : {e}")
                    transfer_retry_count += 1
                    time.sleep(self.retry_sleep_time)

        else:
            log.info(f"file hash of {json_sha512} is the same as is already on S3 and force={force} - not uploading")

        return uploaded_flag

    @typechecked()
    def download(self, s3_key: str, dest_path: Union[str, Path]) -> bool:
        """
        Download an S3 object

        :param s3_key: S3 key
        :param dest_path: destination file path
        :return: True if downloaded successfully
        """

        if isinstance(dest_path, str):
            log.info(f"{dest_path} is not Path object.  Non-Path objects will be deprecated in the future")

        if isinstance(dest_path, Path):
            dest_path = str(dest_path)

        log.info(f'S3 download : {self.bucket_name}/{s3_key} to "{dest_path}" ({Path(dest_path).absolute()})')

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
            except (ClientError, EndpointConnectionError, SSLError, ConnectionClosedError, urllib3.exceptions.ProtocolError) as e:
                # ProtocolError can happen for a broken connection
                log.warning(f"{self.bucket_name}/{s3_key} to {dest_path} ({Path(dest_path).absolute()}) : {transfer_retry_count=} : {e}")
                time.sleep(self.retry_sleep_time)
                transfer_retry_count += 1
        log.debug(sf(transfer_retry_count=transfer_retry_count, success=success, bucket_name=self.bucket_name, s3_key=s3_key, dest_path=dest_path))
        return success

    @typechecked()
    def download_cached(self, s3_key: str, dest_path: Path) -> S3DownloadStatus:
        """
        download from AWS S3 with caching

        :param dest_path: destination full path
        :param s3_key: S3 key of source
        :return: S3DownloadStatus instance
        """

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
            self.download(s3_key, dest_path)
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

        :param dest_path: destination full path
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
        bucket_location = self.client.get_bucket_location(Bucket=self.bucket_name)
        location = bucket_location["LocationConstraint"]
        url = f"https://{self.bucket_name}.s3-{location}.amazonaws.com/{s3_key}"
        return url

    @typechecked()
    def get_s3_object_metadata(self, s3_key: str) -> S3ObjectMetadata:
        """
        Get S3 object metadata

        :param s3_key: S3 key
        :return: S3ObjectMetadata or None if object does not exist
        """
        assert self.resource is not None
        bucket_resource = self.resource.Bucket(self.bucket_name)
        if self.object_exists(s3_key):

            bucket_object = bucket_resource.Object(s3_key)
            assert isinstance(self.bucket_name, str)  # mainly for mypy
            s3_object_metadata = S3ObjectMetadata(
                self.bucket_name,
                s3_key,
                bucket_object.content_length,
                bucket_object.last_modified,
                bucket_object.e_tag[1:-1].lower(),
                bucket_object.metadata.get(sha512_string),
                self.get_s3_object_url(s3_key),
            )

        else:
            raise AWSimpleException(f"{self.bucket_name=} {s3_key=} does not exist")
        log.debug(f"{s3_object_metadata=}")
        return s3_object_metadata

    @typechecked()
    def object_exists(self, s3_key: str) -> bool:
        """
        determine if an s3 object exists

        :param s3_bucket: the S3 bucket
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
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
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

        created = False
        if not self.bucket_exists():
            try:
                if self.public_readable:
                    self.client.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration=location, ACL="public-read")
                else:
                    self.client.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration=location)
                self.client.get_waiter("bucket_exists").wait(Bucket=self.bucket_name)
                created = True
            except ClientError as e:
                log.warning(f"{self.bucket_name=} {e=}")
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
