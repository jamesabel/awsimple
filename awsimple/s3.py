import os
import shutil
import time
from math import isclose
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

from botocore.exceptions import ClientError
from s3transfer import S3UploadFailedError
from typeguard import typechecked
from hashy import get_string_sha512, get_file_sha512, get_file_md5
from balsa import get_logger

from awsimple import AWSAccess, __application_name__, lru_cache_write

# Use this project's name as a prefix to avoid string collisions.  Use dashes instead of underscore since that's AWS's convention.
sha512_string = f"{__application_name__}-sha512"

log = get_logger(__application_name__)


@dataclass
class S3DownloadStatus:
    success: bool = False
    cached: bool = None
    wrote_to_cache: bool = None
    sizes_differ: bool = None
    mtimes_differ: bool = None


@dataclass
class S3ObjectMetadata:
    size: int
    mtime: datetime
    etag: str
    sha512: (str, None)  # hex string - only entries written with awsimple will have this


class S3Access(AWSAccess):

    @typechecked(always=True)
    def __init__(self, bucket_name: str, **kwargs):
        self.bucket_name = bucket_name
        super().__init__(resource_name="s3", **kwargs)

    @typechecked(always=True)
    def download_cached(self, s3_key: str, dest_path: Path) -> S3DownloadStatus:
        """
        download from AWS S3 with caching
        :param dest_path: destination full path.  If this is used, do not pass in dest_dir.
        :param s3_key: S3 key of source
        :return: S3DownloadStatus instance
        """
        status = S3DownloadStatus()

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
                log.info(f"{self.bucket_name}:{s3_key} cache miss: sizes differ {local_size=} {s3_object_metadata.size=}")
                status.cached = False
                status.sizes_differ = True
            elif not isclose(local_mtime, s3_mtime_ts, abs_tol=self.mtime_abs_tol):
                log.info(f"{self.bucket_name}:{s3_key} cache miss: mtimes differ {local_mtime=} {s3_object_metadata.mtime=}")
                status.cached = False
                status.mtimes_differ = True
            else:
                log.info(f"{self.bucket_name}:{s3_key} cache hit : copying {cache_path=} to {dest_path=}")
                status.cached = True
                status.success = True
                shutil.copy2(cache_path, dest_path)
        else:
            status.cached = False

        if not status.cached:
            log.info(f"cache miss : {self.bucket_name=},{s3_key=},{dest_path=}")

            transfer_retry_count = 0

            while not status.success and transfer_retry_count < self.cache_retries:
                try:
                    self.download(s3_key, dest_path)
                    self.cache_dir.mkdir(parents=True, exist_ok=True)
                    status.wrote_to_cache = lru_cache_write(dest_path, self.cache_dir, cache_file_name, self.cache_max_absolute, self.cache_max_of_free)
                    status.success = True
                except ClientError as e:
                    log.warning(f"{self.bucket_name}:{s3_key} to {dest_path=} : {transfer_retry_count=} : {e}")
                    transfer_retry_count += 1
                    time.sleep(3.0)

        return status

    @typechecked(always=True)
    def read_string(self, s3_key: str) -> str:
        log.debug(f"reading {self.bucket_name}/{s3_key}")
        return self.resource.Object(self.bucket_name, s3_key).get()["Body"].read().decode()

    @typechecked(always=True)
    def read_lines(self, s3_key: str) -> list:
        return self.read_string(s3_key).splitlines()

    @typechecked(always=True)
    def write_string(self, input_str: str, s3_key: str):
        log.debug(f"writing {self.bucket_name}/{s3_key}")
        self.resource.Object(self.bucket_name, s3_key).put(Body=input_str)

    @typechecked(always=True)
    def write_lines(self, input_lines: list, s3_key: str):
        self.write_string("\n".join(input_lines), s3_key)

    @typechecked(always=True)
    def delete_object(self, s3_key: str):
        log.info(f"deleting {self.bucket_name}/{s3_key}")
        self.resource.Object(self.bucket_name, s3_key).delete()

    @typechecked(always=True)
    def upload(self, file_path: (str, Path), s3_key: str, force=False) -> bool:

        log.info(f'S3 upload : "{file_path}" to {self.bucket_name}/{s3_key}')

        uploaded_flag = False

        if isinstance(file_path, str):
            file_path = Path(file_path)

        file_mtime = os.path.getmtime(file_path)
        file_md5 = get_file_md5(file_path)
        file_sha512 = get_file_sha512(file_path)
        s3_object_metadata = self.get_s3_object_metadata(s3_key)

        upload_flag = force
        if not upload_flag:
            if s3_object_metadata is None:
                upload_flag = True
            elif s3_object_metadata.sha512 is not None and file_sha512 is not None:
                # use the hash provided by awsimple, if it exists
                upload_flag = file_sha512 != s3_object_metadata.sha512
            else:
                # if not, use mtime
                upload_flag = not isclose(file_mtime, s3_object_metadata.mtime.timestamp(), abs_tol=self.mtime_abs_tol)

        if upload_flag:
            log.info(f"local file : {file_sha512=},{s3_object_metadata=},force={force} - uploading")

            transfer_retry_count = 0
            while not uploaded_flag and transfer_retry_count < 10:
                metadata = {sha512_string: file_sha512}
                log.info(f"{metadata=}")
                try:
                    self.client.upload_file(str(file_path), self.bucket_name, s3_key, ExtraArgs={'Metadata': metadata})
                    uploaded_flag = True
                except S3UploadFailedError as e:
                    log.warning(f"{file_path} to {self.bucket_name}:{s3_key} : {transfer_retry_count=} : {e}")
                    transfer_retry_count += 1
                    time.sleep(1.0)

        else:
            log.info(f"file hash of {file_md5} is the same as is already on S3 and force={force} - not uploading")

        return uploaded_flag

    @typechecked(always=True)
    def download(self, s3_key: str, dest_path: (str, Path)) -> bool:

        if isinstance(dest_path, str):
            log.info(f"{dest_path} is not Path object.  Non-Path objects will be deprecated in the future")

        if isinstance(dest_path, Path):
            dest_path = str(dest_path)

        log.info(f'S3 download : {self.bucket_name}/{s3_key} to "{dest_path}"')

        transfer_retry_count = 0
        success = False
        while not success and transfer_retry_count < 10:
            try:
                self.client.download_file(self.bucket_name, s3_key, dest_path)
                s3_object_metadata = self.get_s3_object_metadata(s3_key)
                mtime_ts = s3_object_metadata.mtime.timestamp()
                os.utime(dest_path, (mtime_ts, mtime_ts))  # set the file mtime to the mtime in S3
                success = True
            except ClientError as e:
                log.warning(f"{self.bucket_name}:{s3_key} to {dest_path} : {transfer_retry_count=} : {e}")
                transfer_retry_count += 1
                time.sleep(1.0)
        return success

    @typechecked(always=True)
    def get_s3_object_metadata(self, s3_key: str) -> (S3ObjectMetadata, None):
        bucket_resource = self.resource.Bucket(self.bucket_name)
        if self.object_exists(s3_key):
            bucket_object = bucket_resource.Object(s3_key)
            s3_object_metadata = S3ObjectMetadata(bucket_object.content_length, bucket_object.last_modified, bucket_object.e_tag[1:-1].lower(), bucket_object.metadata.get(sha512_string))
        else:
            s3_object_metadata = None
        log.debug(f"{s3_object_metadata=}")
        return s3_object_metadata

    @typechecked(always=True)
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

    @typechecked(always=True)
    def bucket_exists(self) -> bool:
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            exists = True
        except ClientError as e:
            log.info(f"{self.bucket_name=}{e=}")
            exists = False
        return exists

    @typechecked(always=True)
    def create_bucket(self) -> bool:
        """
        create S3 bucket
        :return: True if bucket created
        """

        # this is ugly, but create_bucket needs to be told the region explicitly (it doesn't just take it from the config)
        location = {"LocationConstraint": self.get_region()}

        created = False
        if not self.bucket_exists():
            try:
                self.client.create_bucket(Bucket=self.bucket_name, CreateBucketConfiguration=location)
                created = True
            except ClientError as e:
                log.warning(f"{self.bucket_name=} {e=}")
        return created

    @typechecked(always=True)
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
