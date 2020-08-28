import os
import shutil
import time
from math import isclose
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime

from appdirs import user_cache_dir
from botocore.exceptions import ClientError
from s3transfer import S3UploadFailedError
from typeguard import typechecked
from hashy import get_string_sha512, get_file_sha512, get_file_md5

from awsimple import AWSAccess, __application_name__, __author__, lru_cache_write
from awsimple.aws import log

# Use this project's name as a prefix to avoid string collisions.  Use dashes instead of underscore since that's AWS's convention.
sha512_string = f"{__application_name__}-sha512"


@dataclass
class AWSS3DownloadStatus:
    success: bool = False
    cached: bool = None
    wrote_to_cache: bool = None
    sizes_differ: bool = None
    mtimes_differ: bool = None


@dataclass
class AWSS3ObjectMetadata:
    size: int
    mtime: datetime
    etag: str
    sha512: (str, None)  # hex string - only entries written with awsimple will have this


@dataclass
class S3Access(AWSAccess):
    bucket: str = None  # required

    def __post_init__(self):
        if self.bucket is None:
            log.warning(f"{self.bucket=}")

    def get_s3_resource(self):
        return self.get_resource("s3")

    def get_s3_client(self):
        return self.get_client("s3")

    @typechecked(always=True)
    def download_cached(self, dest_path: Path, s3_key: str) -> AWSS3DownloadStatus:
        """
        download from AWS S3 with caching
        :param dest_path: destination full path.  If this is used, do not pass in dest_dir.
        :param s3_key: S3 key of source
        :param cache_dir: cache dir
        :param retries: number of times to retry the AWS S3 access
        :return: AWSS3DownloadStatus instance
        """
        status = AWSS3DownloadStatus()

        # use a hash of the S3 address so we don't have to try to store the local object (file) in a hierarchical directory tree
        # use the slash to distinguish between bucket and key, since that's most like the actual URL AWS uses
        # https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html
        cache_file_name = get_string_sha512(f"{self.bucket}/{s3_key}")

        if self.cache_dir is None:
            self.cache_dir = Path(user_cache_dir(__application_name__, __author__), "aws", "s3")
        cache_path = Path(self.cache_dir, cache_file_name)
        log.debug(f"{cache_path}")

        if cache_path.exists():
            s3_object_metadata = self.get_s3_object_metadata(s3_key)
            s3_mtime_ts = s3_object_metadata.mtime.timestamp()
            local_size = os.path.getsize(cache_path)
            local_mtime = os.path.getmtime(cache_path)

            if local_size != s3_object_metadata.size:
                log.info(f"{self.bucket}:{s3_key} cache miss: sizes differ {local_size=} {s3_object_metadata.size=}")
                status.cached = False
                status.sizes_differ = True
            elif not isclose(local_mtime, s3_mtime_ts, abs_tol=self.abs_tol):
                log.info(f"{self.bucket}:{s3_key} cache miss: mtimes differ {local_mtime=} {s3_object_metadata.mtime=}")
                status.cached = False
                status.mtimes_differ = True
            else:
                status.cached = True
                status.success = True
                shutil.copy2(cache_path, dest_path)
        else:
            status.cached = False

        if not status.cached:
            log.info(f"S3 download : {self.bucket=},{s3_key=},{dest_path=}")

            transfer_retry_count = 0

            while not status.success and transfer_retry_count < self.cache_retries:
                try:
                    self.download(dest_path, s3_key)
                    self.cache_dir.mkdir(parents=True, exist_ok=True)
                    status.wrote_to_cache = lru_cache_write(dest_path, self.cache_dir, cache_file_name, self.cache_max_absolute, self.cache_max_of_free)
                    status.success = True
                except ClientError as e:
                    log.warning(f"{self.bucket}:{s3_key} to {dest_path=} : {transfer_retry_count=} : {e}")
                    transfer_retry_count += 1
                    time.sleep(3.0)

        return status

    @typechecked(always=True)
    def read_string(self, s3_key: str) -> str:
        log.debug(f"reading {self.bucket}:{s3_key}")
        s3 = self.get_s3_resource()
        return s3.Object(self.bucket, s3_key).get()["Body"].read().decode()

    @typechecked(always=True)
    def read_lines(self, s3_key: str) -> list:
        return self.read_string(s3_key).splitlines()

    @typechecked(always=True)
    def write_string(self, input_str: str, s3_key: str):
        log.debug(f"writing {self.bucket}:{s3_key}")
        s3 = self.get_s3_resource()
        s3.Object(self.bucket, s3_key).put(Body=input_str)

    @typechecked(always=True)
    def write_lines(self, input_lines: list, s3_key: str):
        self.write_string("\n".join(input_lines), s3_key)

    @typechecked(always=True)
    def delete_object(self, s3_key: str):
        log.info(f"deleting {self.bucket}:{s3_key}")
        s3 = self.get_s3_resource()
        s3.Object(self.bucket, s3_key).delete()

    @typechecked(always=True)
    def upload(self, file_path: (str, Path), s3_key: str, force=False) -> bool:

        log.info(f"S3 upload : file_path={file_path} : bucket={self.bucket} : key={s3_key}")

        uploaded_flag = False

        if isinstance(file_path, str):
            file_path = Path(file_path)

        file_mtime = os.path.getmtime(file_path)
        file_md5 = get_file_md5(file_path)
        file_sha512 = get_file_sha512(file_path)
        s3_object_metadata = self.get_s3_object_metadata(s3_key)

        upload_flag = force
        if not upload_flag:
            if s3_object_metadata.sha512 is not None and file_sha512 is not None:
                # use the hash provided by awsimple, if it exists
                upload_flag = file_sha512 != s3_object_metadata.sha512
            else:
                # if not, use mtime
                upload_flag = isclose(file_mtime, s3_object_metadata.mtime.timestamp(), abs_tol=self.abs_tol)

        if upload_flag:
            log.info(f"local file : {file_sha512=},{s3_object_metadata.sha512=},force={force} - uploading")
            s3_client = self.get_client("s3")

            transfer_retry_count = 0
            while not uploaded_flag and transfer_retry_count < 10:
                metadata = {sha512_string: file_sha512}
                log.info(f"{metadata=}")
                try:
                    s3_client.upload_file(str(file_path), self.bucket, s3_key, ExtraArgs={'Metadata': metadata})
                    uploaded_flag = True
                except S3UploadFailedError as e:
                    log.warning(f"{file_path} to {self.bucket}:{s3_key} : {transfer_retry_count=} : {e}")
                    transfer_retry_count += 1
                    time.sleep(1.0)

        else:
            log.info(f"file hash of {file_md5} is the same as is already on S3 and force={force} - not uploading")

        return uploaded_flag

    @typechecked(always=True)
    def download(self, file_path: (str, Path), s3_key: str) -> bool:

        if isinstance(file_path, str):
            log.info(f"{file_path} is not Path object.  Non-Path objects will be deprecated in the future")

        if isinstance(file_path, Path):
            file_path = str(file_path)

        log.info(f"S3 download : file_path={file_path} : bucket={self.bucket} : key={s3_key}")
        s3_client = self.get_client("s3")

        transfer_retry_count = 0
        success = False
        while not success and transfer_retry_count < 10:
            try:
                s3_client.download_file(self.bucket, s3_key, file_path)
                s3_object_metadata = self.get_s3_object_metadata(s3_key)
                mtime_ts = s3_object_metadata.mtime.timestamp()
                os.utime(file_path, (mtime_ts, mtime_ts))  # set the file mtime to the mtime in S3
                success = True
            except ClientError as e:
                log.warning(f"{self.bucket}:{s3_key} to {file_path} : {transfer_retry_count=} : {e}")
                transfer_retry_count += 1
                time.sleep(1.0)
        return success

    @typechecked(always=True)
    def get_s3_object_metadata(self, s3_key: str) -> (AWSS3ObjectMetadata, None):
        s3_resource = self.get_s3_resource()
        bucket_resource = s3_resource.Bucket(self.bucket)
        if self.object_exists(s3_key):
            bucket_object = bucket_resource.Object(s3_key)
            s3_object_metadata = AWSS3ObjectMetadata(bucket_object.content_length, bucket_object.last_modified, bucket_object.e_tag[1:-1].lower(), bucket_object.metadata.get(sha512_string))
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
        s3_resource = self.get_s3_resource()
        bucket_resource = s3_resource.Bucket(self.bucket)
        objs = list(bucket_resource.objects.filter(Prefix=s3_key))
        object_exists = len(objs) > 0 and objs[0].key == s3_key
        log.debug(f"{self.bucket}:{s3_key} : {object_exists=}")
        return object_exists

    @typechecked(always=True)
    def bucket_exists(self) -> bool:
        s3_client = self.get_s3_client()
        try:
            s3_client.head_bucket(Bucket=self.bucket)
            exists = True
        except ClientError as e:
            log.info(f"{self.bucket=}{e=}")
            exists = False
        return exists

    @typechecked(always=True)
    def create_bucket(self) -> bool:
        """
        create S3 bucket
        :return: True if bucket created
        """
        s3_client = self.get_s3_client()

        # this is ugly, but create_bucket needs to be told the region explicitly (it doesn't just take it from the config)
        location = {"LocationConstraint": self.get_region()}

        created = False
        if not self.bucket_exists():
            try:
                s3_client.create_bucket(Bucket=self.bucket, CreateBucketConfiguration=location)
                created = True
            except ClientError as e:
                log.warning(f"{self.bucket=} {e=}")
        return created

    @typechecked(always=True)
    def delete_bucket(self) -> bool:
        """
        delete S3 bucket
        :return: True if bucket deleted (False if didn't exist in the first place)
        """
        try:
            s3_client = self.get_s3_client()
            s3_client.delete_bucket(Bucket=self.bucket)
            deleted = True
        except ClientError as e:
            log.info(f"{self.bucket=}{e=}")  # does not exist
            deleted = False
        return deleted
