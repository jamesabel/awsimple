import hashlib
import os
from pathlib import Path

from test_awsimple import temp_dir


def test_s3_native_checksum_write_string(s3_access):
    # objects written by awsimple get S3's native SHA-512 checksum, which reads back as the whole-object hex hash
    content = "native checksum test"
    s3_key = "native_checksum_test.txt"
    s3_access.write_string(content, s3_key)
    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.sha512 == hashlib.sha512(content.encode()).hexdigest()


def test_s3_native_checksum_without_custom_metadata(s3_access):
    # an object written by some other tool with a native checksum (but no awsimple custom metadata) still gets a usable sha512
    content = b"written by some other tool"
    s3_key = "native_checksum_no_metadata.txt"
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=content, ChecksumAlgorithm="SHA512")
    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.sha512 == hashlib.sha512(content).hexdigest()


def test_s3_no_checksum_no_metadata(s3_access):
    # neither a native SHA-512 checksum nor custom metadata - .sha512 is None and get_sha512() provides the substitute hash
    content = b"no checksum at all"
    s3_key = "no_checksum_at_all.txt"
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=content)
    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.sha512 is None
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
    assert metadata.legacy_sha512 == content_sha512  # legacy metadata hash detected

    # upload the identical content - it would be skipped if the native checksum existed, but legacy objects always migrate
    file_path = Path(temp_dir, "legacy_metadata_object.txt")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(content)
    assert s3_access.upload(file_path, s3_key)  # re-uploaded (migrated), even though the content is unchanged

    metadata = s3_access.get_s3_object_metadata(s3_key)
    assert metadata.sha512 == content_sha512  # now has the native checksum
    assert metadata.legacy_sha512 is None  # legacy metadata is gone
    assert not s3_access.upload(file_path, s3_key)  # migration complete - unchanged content is now skipped


def test_s3_no_hash_mtime_and_size_comparison(s3_access):
    # when the object has no hash of any kind, change detection falls back to modification time and file size
    content = b"0123456789"
    s3_key = "no_hash_mtime_size.txt"
    s3_access.client.put_object(Bucket=s3_access.bucket_name, Key=s3_key, Body=content)  # no hash of any kind
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
    assert metadata.sha512 == hashlib.sha512(b"upload me").hexdigest()
    assert not s3_access.upload(file_path, s3_key)  # unchanged, so no upload happens
    file_path.write_text("upload me again")
    assert s3_access.upload(file_path, s3_key)  # changed, so it uploads
