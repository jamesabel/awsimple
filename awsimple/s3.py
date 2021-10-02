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
import urllib3
from logging import getLogger

from botocore.exceptions import ClientError, EndpointConnectionError
from s3transfer import S3UploadFailedError
from typeguard import typechecked
from hashy import get_string_sha512, get_file_sha512  # type: ignore

from awsimple import CacheAccess, __application_name__, lru_cache_write, AWSimpleException

# Use this project's name as a prefix to avoid string collisions.  Use dashes instead of underscore since that's AWS's convention.
sha512_string = f"{__application_name__}-sha512"

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
    sizes_differ: Union[bool, None] = None
    mtimes_differ: Union[bool, None] = None


@dataclass
class S3ObjectMetadata:
    key: str
    size: int
    mtime: datetime
    etag: str  # generally not used
    sha512: Union[str, None]  # hex string - only entries written with awsimple will have this
    url: str  # URL of S3 object


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
    def download_cached(self, s3_key: str, dest_path: Path) -> S3DownloadStatus:
        """
        download from AWS S3 with caching

        :param dest_path: destination full path
        :param s3_key: S3 key of source
        :return: S3DownloadStatus instance
        """

        self.download_status = S3DownloadStatus()  # init

        # use a hash of the S3 address so we don't have to try to store the local object (file) in a hierarchical directory tree
        # use the slash to distinguish between bucket and key, since that's most like the actual URL AWS uses
        # https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
        cache_file_name = get_string_sha512(f"{self.bucket_name}/{s3_key}")

        cache_path = Path(self.cache_dir, cache_file_name)
        log.debug(f"{cache_path}")

        if cache_path.exists():
            s3_object_metadata = self.get_s3_object_metadata(s3_key)
            s3_mtime_ts = s3_object_metadata.mtime.timestamp()
            local_size = os.path.getsize(cache_path)
            local_mtime = os.path.getmtime(cache_path)

            if local_size != s3_object_metadata.size:
                log.info(f"{self.bucket_name}/{s3_key} cache miss: sizes differ {local_size=} {s3_object_metadata.size=}")
                self.download_status.cache_hit = False
                self.download_status.sizes_differ = True
            elif not isclose(local_mtime, s3_mtime_ts, abs_tol=self.mtime_abs_tol):
                log.info(f"{self.bucket_name}/{s3_key} cache miss: mtimes differ {local_mtime=} {s3_object_metadata.mtime=}")
                self.download_status.cache_hit = False
                self.download_status.mtimes_differ = True
            else:
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
            self.download_status.cache_write = lru_cache_write(dest_path, self.cache_dir, cache_file_name, self.cache_max_absolute, self.cache_max_of_free)
            self.download_status.success = True

        return self.download_status

    @typechecked()
    def read_string(self, s3_key: str) -> str:
        """
        Read contents of an S3 object as a string

        :param s3_key: S3 key
        :return: S3 object as a string
        """
        log.debug(f"reading {self.bucket_name}/{s3_key}")
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
        self.resource.Object(self.bucket_name, s3_key).delete()

    @typechecked()
    def upload(self, file_path: Union[str, Path], s3_key: str, force=False) -> bool:
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
                if s3_object_metadata.sha512 is not None and file_sha512 is not None:
                    # use the hash provided by awsimple, if it exists
                    upload_flag = file_sha512 != s3_object_metadata.sha512
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
                    extra_args["ACL"] = "public-read"
                log.info(f"{extra_args=}")
                try:
                    self.client.upload_file(str(file_path), self.bucket_name, s3_key, ExtraArgs=extra_args)
                    uploaded_flag = True
                except (S3UploadFailedError, ClientError, EndpointConnectionError, urllib3.exceptions.ProtocolError) as e:
                    log.warning(f"{file_path} to {self.bucket_name}:{s3_key} : {transfer_retry_count=} : {e}")
                    transfer_retry_count += 1
                    time.sleep(self.retry_sleep_time)

        else:
            log.info(f"file hash of {file_sha512} is the same as is already on S3 and force={force} - not uploading")

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
                self.client.download_file(self.bucket_name, s3_key, dest_path)
                s3_object_metadata = self.get_s3_object_metadata(s3_key)
                mtime_ts = s3_object_metadata.mtime.timestamp()
                os.utime(dest_path, (mtime_ts, mtime_ts))  # set the file mtime to the mtime in S3
                success = True
            except (ClientError, EndpointConnectionError, urllib3.exceptions.ProtocolError) as e:
                # ProtocolError can happen for a broken connection
                log.warning(f"{self.bucket_name}/{s3_key} to {dest_path} ({Path(dest_path).absolute()}) : {transfer_retry_count=} : {e}")
                transfer_retry_count += 1
                time.sleep(self.retry_sleep_time)
        return success

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
        bucket_resource = self.resource.Bucket(self.bucket_name)
        if self.object_exists(s3_key):

            bucket_object = bucket_resource.Object(s3_key)
            s3_object_metadata = S3ObjectMetadata(
                s3_key, bucket_object.content_length, bucket_object.last_modified, bucket_object.e_tag[1:-1].lower(), bucket_object.metadata.get(sha512_string), self.get_s3_object_url(s3_key)
            )

        else:
            raise AWSimpleException(f"{s3_key} does not exist")
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
    def dir(self) -> Dict[str, S3ObjectMetadata]:
        """
        Do a "directory" of an S3 bucket where the returned dict key is the S3 key and the value is an S3ObjectMetadata object.

        Use the faster .keys() method if all you need are the keys.

        :return: a dict where key is the S3 key and the value is S3ObjectMetadata
        """
        directory = {}
        if self.bucket_exists():
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name):
                # deal with empty bucket
                for content in page.get("Contents", []):
                    s3_key = content.get("Key")
                    directory[s3_key] = self.get_s3_object_metadata(s3_key)
        else:
            raise BucketNotFound(self.bucket_name)
        return directory

    def keys(self) -> List[str]:
        """
        List all the keys on this S3 Bucket.

        Note that this should be faster than .dir() if all you need are the keys and not the metadata.

        :return: a sorted list of all the keys in this S3 Bucket (sorted for consistency)
        """
        keys = []
        if self.bucket_exists():
            paginator = self.client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self.bucket_name):
                # deal with empty bucket
                for content in page.get("Contents", []):
                    s3_key = content.get("Key")
                    keys.append(s3_key)
        else:
            raise BucketNotFound(self.bucket_name)
        keys.sort()
        return keys
