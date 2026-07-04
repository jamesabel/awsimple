import hashlib
import os
from pathlib import Path

from awsimple import get_bytes_crc64nvme, get_file_crc64nvme

from test_awsimple import temp_dir


def test_s3_native_checksum_write_string(s3_access):
    # objects written by awsimple get S3's native full-object CRC64NVME checksum, which reads back as the whole-object hex value
    content = "native checksum test"
    s3_key = "native_checksum_test.txt"
    s3_access.write_string(content, s3_key)
    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.crc64nvme == get_bytes_crc64nvme(content.encode())
    assert metadata.get_sha512() == metadata.crc64nvme  # the checksum is also the cache key


def test_s3_foreign_object_with_native_sha512(s3_access):
    # an object written by some other tool with a native SHA-512 checksum is still recognized and comparable
    content = b"written by some other tool"
    s3_key = "native_checksum_sha512.txt"
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=content, ChecksumAlgorithm="SHA512")
    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.sha512 == hashlib.sha512(content).hexdigest()
    assert metadata.crc64nvme is None

    # uploading identical content compares via the object's SHA-512 and is skipped
    file_path = Path(temp_dir, "native_checksum_sha512.txt")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(content)
    assert not s3_access.upload(file_path, s3_key)


def test_s3_no_full_object_hash(s3_access):
    # no full-object hash awsimple understands (raw put gets the SDK default CRC32) - .sha512/.crc64nvme are None and get_sha512() provides the substitute hash
    content = b"no full-object hash"
    s3_key = "no_full_object_hash.txt"
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=content)
    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.sha512 is None
    assert metadata.crc64nvme is None
    assert metadata.legacy_sha512 is None
    assert metadata.get_sha512() is not None  # substitute hash so caching still works


def test_s3_legacy_metadata_object_always_reuploaded(s3_access):
    # an object written by an older awsimple (legacy metadata hash, no native checksum) is always re-uploaded so it gains the native checksum
    content = b"legacy object"
    content_sha512 = hashlib.sha512(content).hexdigest()
    s3_key = "legacy_metadata_object.txt"
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=content, Metadata={"awsimple-sha512": content_sha512})

    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.sha512 is None  # no native checksum
    assert metadata.crc64nvme is None
    assert metadata.legacy_sha512 == content_sha512  # legacy metadata hash detected

    # upload the identical content - it would be skipped if a native checksum existed, but legacy objects always migrate
    file_path = Path(temp_dir, "legacy_metadata_object.txt")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(content)
    assert s3_access.upload(file_path, s3_key)  # re-uploaded (migrated), even though the content is unchanged

    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.crc64nvme == get_bytes_crc64nvme(content)  # now has the native checksum
    assert metadata.legacy_sha512 is None  # legacy metadata is gone
    assert not s3_access.upload(file_path, s3_key)  # migration complete - unchanged content is now skipped


def test_s3_no_hash_mtime_and_size_comparison(s3_access):
    # when the object has no full-object hash, change detection falls back to modification time and file size
    content = b"0123456789"
    s3_key = "no_hash_mtime_size.txt"
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=content)  # no full-object hash
    mtime_ts = s3_access.get_s3_object_metadata(s3_key).mtime.timestamp()

    file_path = Path(temp_dir, "no_hash_mtime_size.txt")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(content)
    os.utime(file_path, (mtime_ts, mtime_ts))
    assert not s3_access.upload(file_path, s3_key)  # same mtime and same size - no upload

    file_path.write_bytes(content + b"more")
    os.utime(file_path, (mtime_ts, mtime_ts))
    assert s3_access.upload(file_path, s3_key)  # same mtime but different size - uploads


def test_s3_native_checksum_upload_skip(s3_access):
    # change detection via the native checksum: an unchanged file is not re-uploaded
    file_path = Path(temp_dir, "native_checksum_upload.txt")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("upload me")
    s3_key = "native_checksum_upload.txt"
    assert s3_access.upload(file_path, s3_key, force=True)
    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.crc64nvme == get_bytes_crc64nvme(b"upload me")
    assert not s3_access.upload(file_path, s3_key)  # unchanged, so no upload happens
    file_path.write_text("upload me again")
    assert s3_access.upload(file_path, s3_key)  # changed, so it uploads


def test_s3_multipart_full_object_checksum(s3_access):
    # multipart uploads (above the 8 MiB threshold) still get a full-object CRC64NVME checksum, so change detection works at any size
    file_path = Path(temp_dir, "multipart_checksum.bin")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(os.urandom(20 * 1024 * 1024))  # 20 MB - uploads as multiple parts
    s3_key = "multipart_checksum.bin"

    assert s3_access.upload(file_path, s3_key, force=True)
    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.crc64nvme == get_file_crc64nvme(file_path)  # full-object checksum, not a composite
    assert not s3_access.upload(file_path, s3_key)  # unchanged, so no upload happens
