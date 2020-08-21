import os
import shutil
import time
from math import isclose
from pathlib import Path
from dataclasses import dataclass

from appdirs import user_cache_dir
from botocore.exceptions import ClientError
from s3transfer import S3Transfer, S3UploadFailedError

from awsimple import AWSAccess, get_string_sha512, get_file_md5, __application_name__, __author__
from awsimple.aws import log


@dataclass
class AWSS3DownloadStatus:
    success: bool = False
    cached: bool = None
    sizes_differ: bool = None
    mtimes_differ: bool = None


@dataclass
class S3Access(AWSAccess):
    bucket: str = None  # required
    cache_abs_tol: float = 3.0  # file modification times within this cache window (in seconds) are considered equivalent

    def __post_init__(self):
        if self.bucket is None:
            log.warning(f"{self.bucket=}")

    def get_s3_resource(self):
        return self.get_resource("s3")

    def get_s3_client(self):
        return self.get_client("s3")

    def download_cached(self, s3_key: str, dest_dir: (Path, None), dest_path: (Path, None), cache_dir: (Path, None), retries: int = 10) -> AWSS3DownloadStatus:
        """
        download from AWS S3 with caching
        :param s3_bucket: S3 bucket of source
        :param s3_key: S3 key of source
        :param dest_dir: destination directory.  If given, the destination full path is the dest_dir and s3_key (in this case s3_key must not have slashes).  If dest_dir is used,
                         do not pass in dest_path.
        :param dest_path: destination full path.  If this is used, do not pass in dest_dir.
        :param cache_dir: cache dir
        :param retries: number of times to retry the AWS S3 access
        :return: AWSS3DownloadStatus instance
        """
        status = AWSS3DownloadStatus()

        if (dest_dir is None and dest_path is None) or (dest_dir is not None and dest_path is not None):
            log.error(f"{dest_dir=} and {dest_path=}")
        else:

            if dest_dir is not None and dest_path is None:
                if "/" in s3_key or "\\" in s3_key:
                    log.error(f"slash (/ or \\) in s3_key '{s3_key}' - can not download {self.bucket}:{s3_key}")
                else:
                    dest_path = Path(dest_dir, s3_key)

            if dest_path is not None:

                # use a hash of the S3 address so we don't have to try to store the local object (file) in a hierarchical directory tree
                cache_file_name = get_string_sha512(f"{self.bucket}{s3_key}")

                if cache_dir is None:
                    cache_dir = Path(user_cache_dir(__application_name__, __author__, "aws", "s3"))
                cache_path = Path(cache_dir, cache_file_name)
                log.debug(f"{cache_path}")

                if cache_path.exists():
                    s3_size, s3_mtime, s3_hash = self.get_size_mtime_hash(s3_key)
                    local_size = os.path.getsize(cache_path)
                    local_mtime = os.path.getmtime(cache_path)

                    if local_size != s3_size:
                        log.info(f"{self.bucket}:{s3_key} cache miss: sizes differ {local_size=} {s3_size=}")
                        status.cached = False
                        status.sizes_differ = True
                    elif not isclose(local_mtime, s3_mtime, abs_tol=self.cache_abs_tol):
                        log.info(f"{self.bucket}:{s3_key} cache miss: mtimes differ {local_mtime=} {s3_mtime=}")
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
                    s3_client = self.get_client("s3")
                    transfer = S3Transfer(s3_client)

                    transfer_retry_count = 0

                    while not status.success and transfer_retry_count < retries:
                        try:
                            transfer.download_file(self.bucket, s3_key, dest_path)
                            shutil.copy2(dest_path, cache_path)
                            status.success = True
                        except ClientError as e:
                            log.warning(f"{self.bucket}:{s3_key} to {dest_path=} : {transfer_retry_count=} : {e}")
                            transfer_retry_count += 1
                            time.sleep(3.0)

        return status

    def read_string(self, s3_key: str) -> str:
        log.debug(f"reading {self.bucket}:{s3_key}")
        s3 = self.get_s3_resource()
        return s3.Object(self.bucket, s3_key).get()["Body"].read().decode()

    def read_lines(self, s3_key: str) -> list:
        return self.read_string(s3_key).splitlines()

    def write_string(self, input_str: str, s3_key: str):
        log.debug(f"writing {self.bucket}:{s3_key}")
        s3 = self.get_s3_resource()
        s3.Object(self.bucket, s3_key).put(Body=input_str)

    def write_lines(self, input_lines: list, s3_key: str):
        self.write_string("\n".join(input_lines), s3_key)

    def delete_object(self, s3_key: str):
        log.debug(f"deleting {self.bucket}:{s3_key}")
        s3 = self.get_s3_resource()
        s3.Object(self.bucket, s3_key).delete()

    def upload(self, file_path: (str, Path), s3_key: str, force=False) -> bool:

        log.info(f"S3 upload : file_path={file_path} : bucket={self.bucket} : key={s3_key}")

        uploaded_flag = False

        if isinstance(file_path, str):
            file_path = Path(file_path)

        file_md5 = get_file_md5(file_path)
        _, _, s3_md5 = self.get_size_mtime_hash(s3_key)

        if file_md5 != s3_md5 or force:
            log.info(f"file hash of local file is {file_md5} and the S3 etag is {s3_md5} , force={force} - uploading")
            s3_client = self.get_client("s3")
            transfer = S3Transfer(s3_client)

            transfer_retry_count = 0
            while not uploaded_flag and transfer_retry_count < 10:
                try:
                    transfer.upload_file(file_path, self.bucket, s3_key)
                    uploaded_flag = True
                except S3UploadFailedError as e:
                    log.warning(f"{file_path} to {self.bucket}:{s3_key} : {transfer_retry_count=} : {e}")
                    transfer_retry_count += 1
                    time.sleep(1.0)

        else:
            log.info(f"file hash of {file_md5} is the same as is already on S3 and force={force} - not uploading")

        return uploaded_flag

    def download(self, file_path: (str, Path), s3_key: str) -> bool:

        if isinstance(file_path, str):
            log.info(f"{file_path} is not Path object.  Non-Path objects will be deprecated in the future")

        if isinstance(file_path, Path):
            file_path = str(file_path)

        log.info(f"S3 download : file_path={file_path} : bucket={self.bucket} : key={s3_key}")
        s3_client = self.get_client("s3")
        transfer = S3Transfer(s3_client)

        transfer_retry_count = 0
        success = False
        while not success and transfer_retry_count < 10:
            try:
                transfer.download_file(self.bucket, s3_key, file_path)
                success = True
            except ClientError as e:
                log.warning(f"{self.bucket}:{s3_key} to {file_path} : {transfer_retry_count=} : {e}")
                transfer_retry_count += 1
                time.sleep(1.0)
        return success

    def get_size_mtime_hash(self, s3_key: str) -> tuple:
        s3_resource = self.get_s3_resource()
        bucket_resource = s3_resource.Bucket(self.bucket)

        # determine if the object exists before we try to get the info
        objs = list(bucket_resource.objects.filter(Prefix=s3_key))
        if len(objs) > 0 and objs[0].key == s3_key:
            bucket_object = bucket_resource.Object(s3_key)
            object_size = bucket_object.content_length
            object_mtime = bucket_object.last_modified
            object_hash = bucket_object.e_tag[1:-1].lower()
        else:
            object_size = None
            object_mtime = None
            object_hash = None  # does not exist
        log.debug(f"size : {object_size} ,  mtime : {object_mtime} , hash : {object_hash}")
        return object_size, object_mtime, object_hash

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

    def bucket_exists(self) -> bool:
        s3_client = self.get_s3_client()
        try:
            s3_client.head_bucket(Bucket=self.bucket)
            exists = True
        except ClientError as e:
            log.info(f"{self.bucket=}{e=}")
            exists = False
        return exists

    def create_bucket(self) -> bool:
        s3_client = self.get_s3_client()

        # this is ugly, but create_bucket needs to be told the region explicitly (it doesn't just take it from the config)
        location = {'LocationConstraint': self.get_region()}

        try:
            s3_client.create_bucket(Bucket=self.bucket, CreateBucketConfiguration=location)
            created = True
        except ClientError as e:
            log.info(f"{self.bucket=}{e=}")  # exists
            created = False
        return created

    def delete_bucket(self) -> bool:
        try:
            s3_client = self.get_s3_client()
            s3_client.delete_bucket(Bucket=self.bucket)
            deleted = True
        except ClientError as e:
            log.info(f"{self.bucket=}{e=}")  # does not exist
            deleted = False
        return deleted
