"""
The hashes of an S3 object that awsimple writes are hashes of the whole file, whatever way it was uploaded, and the names awsimple uses for them.

All tests, fixtures, helpers and constants in this file are AI-generated (Claude Code).
"""

import dataclasses
import hashlib
import os
from pathlib import Path

from boto3.s3.transfer import TransferConfig
from hashy import get_file_crc64nvme

import awsimple
from awsimple import S3Access, S3ObjectMetadata
from awsimple.s3 import default_multipart_threshold, sha512_string
from test_awsimple import test_awsimple_str


class _SinglePartUpload(S3Access):
    def get_s3_transfer_config(self) -> TransferConfig:
        return TransferConfig(use_threads=False, multipart_threshold=1024 * 1024 * 1024)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_hashes_do_not_depend_on_how_a_file_is_uploaded(s3_access, tmp_path):
    # the same file uploaded in one part and in several parts has the same CRC64NVME and SHA-512: those of the whole file
    file_path = Path(tmp_path, "file.bin")
    file_path.write_bytes(os.urandom(default_multipart_threshold + 1024 * 1024))
    file_sha512 = hashlib.sha512(file_path.read_bytes()).hexdigest()
    file_crc64nvme = get_file_crc64nvme(file_path)

    single_part_s3_access = _SinglePartUpload(profile_name=test_awsimple_str, bucket_name=test_awsimple_str, cache_dir=s3_access.cache_dir)
    assert s3_access.upload(file_path, "file_based_hashes/multipart.bin", force=True)
    assert single_part_s3_access.upload(file_path, "file_based_hashes/single_part.bin", force=True)
    multipart = s3_access.get_s3_object_metadata("file_based_hashes/multipart.bin")
    single_part = s3_access.get_s3_object_metadata("file_based_hashes/single_part.bin")
    assert "-" in multipart.etag and "-" not in single_part.etag  # they were uploaded in different ways

    for s3_object_metadata in (multipart, single_part):
        assert s3_object_metadata.crc64nvme == file_crc64nvme
        assert s3_object_metadata.metadata_sha512 == file_sha512
        assert s3_object_metadata.get_content_sha512() == file_sha512
        assert s3_object_metadata.get_sha512() == file_sha512
        assert s3_object_metadata.size == file_path.stat().st_size

    # so the file is not uploaded again, to either of them, by either way of uploading
    for uploader in (s3_access, single_part_s3_access):
        for s3_key in ("file_based_hashes/multipart.bin", "file_based_hashes/single_part.bin"):
            assert not uploader.upload(file_path, s3_key)

    # and they are the same contents in the cache
    assert not s3_access.download_cached("file_based_hashes/multipart.bin", Path(tmp_path, "multipart.bin")).cache_hit
    assert s3_access.download_cached("file_based_hashes/single_part.bin", Path(tmp_path, "single_part.bin")).cache_hit
    assert Path(tmp_path, "single_part.bin").read_bytes() == file_path.read_bytes()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_metadata_sha512_name():
    # the SHA-512 that awsimple writes on every S3 object is S3ObjectMetadata.metadata_sha512, from the "awsimple-sha512" object metadata
    assert sha512_string == "awsimple-sha512"
    field_names = [field.name for field in dataclasses.fields(S3ObjectMetadata)]
    assert "metadata_sha512" in field_names
    assert field_names[:8] == ["bucket", "key", "size", "mtime", "etag", "sha512", "url", "metadata_sha512"]  # positional order


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_nothing_is_named_as_out_of_date():
    # owner's rule: nothing in awsimple has the word (spelled here in two parts so that this file does not have it) in its name
    word = "leg" + "acy"
    package_dir = Path(awsimple.__file__).parent
    source_paths = sorted(package_dir.rglob("*.py")) + sorted(Path(__file__).parent.rglob("*.py"))
    assert len(source_paths) > 20
    assert [str(source_path) for source_path in source_paths if word in source_path.name.lower() or word in source_path.read_text(encoding="utf-8").lower()] == []
    assert [name for name in dir(awsimple) if word in name.lower()] == []
