"""
An S3Access counts the S3 requests it makes and the bytes S3 sends it (S3Access.usage), which is what AWS charges for.

All tests, fixtures, helpers and constants in this file are AI-generated (Claude Code).
"""

from threading import Thread

import pytest
from botocore.exceptions import ClientError

from awsimple import S3Access, S3RequestTier, S3SyncOutcome, S3Usage, S3VerifyOutcome, get_s3_request_tier, standard_storage_class
from test_awsimple import test_awsimple_str, cache_dir

usage_prefix = "usage/"
infrequent_access = "STANDARD_IA"


def _make_s3_access() -> S3Access:
    # an S3Access of its own, so that what a test counts is only what the test did
    return S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str, cache_dir=cache_dir)


def _write(s3_access: S3Access, prefix: str, contents: dict):
    for name, content in contents.items():
        s3_access.write_string(content, f"{prefix}{name}")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.parametrize(
    "operation,tier",
    [
        ("HeadObject", S3RequestTier.tier2),
        ("GetObject", S3RequestTier.tier2),
        ("HeadBucket", S3RequestTier.tier2),
        ("GetBucketLocation", S3RequestTier.tier2),
        ("ListObjectsV2", S3RequestTier.tier1),
        ("ListBuckets", S3RequestTier.tier1),
        ("PutObject", S3RequestTier.tier1),
        ("CopyObject", S3RequestTier.tier1),
        ("CreateMultipartUpload", S3RequestTier.tier1),
        ("UploadPart", S3RequestTier.tier1),
        ("CompleteMultipartUpload", S3RequestTier.tier1),
        ("DeleteObject", S3RequestTier.free),
        ("AbortMultipartUpload", S3RequestTier.free),
    ],
)
def test_get_s3_request_tier(operation, tier):
    assert get_s3_request_tier(operation) == tier


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_usage_counts():
    usage = S3Usage()
    assert usage.get_request_counts() == {}
    assert usage.get_request_count() == 0
    assert usage.get_tier_counts() == {}
    assert usage.get_download_sizes() == {}
    assert usage.get_download_size() == 0

    usage.record("HeadObject")
    usage.record("HeadObject", "GLACIER_IR")
    usage.record("HeadObject", "GLACIER_IR")
    usage.record("GetObject", "GLACIER_IR", 100)
    usage.record("GetObject", download_size=20)
    usage.record("GetObject", download_size=3)
    usage.record("ListObjectsV2")
    usage.record("DeleteObject")

    assert usage.get_request_counts() == {
        ("HeadObject", standard_storage_class): 1,
        ("HeadObject", "GLACIER_IR"): 2,
        ("GetObject", "GLACIER_IR"): 1,
        ("GetObject", standard_storage_class): 2,
        ("ListObjectsV2", standard_storage_class): 1,
        ("DeleteObject", standard_storage_class): 1,
    }
    assert usage.get_request_count() == 8
    assert usage.get_request_count("HeadObject") == 3
    assert usage.get_request_count(storage_class="GLACIER_IR") == 3
    assert usage.get_request_count("HeadObject", "GLACIER_IR") == 2
    assert usage.get_request_count("PutObject") == 0
    assert usage.get_tier_counts() == {
        (S3RequestTier.tier2, standard_storage_class): 3,
        (S3RequestTier.tier2, "GLACIER_IR"): 3,
        (S3RequestTier.tier1, standard_storage_class): 1,
        (S3RequestTier.free, standard_storage_class): 1,
    }
    assert usage.get_download_sizes() == {"GLACIER_IR": 100, standard_storage_class: 23}
    assert usage.get_download_size() == 123

    usage.reset()
    assert usage.get_request_counts() == {}
    assert usage.get_download_sizes() == {}


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_usage_counts_from_many_threads():
    # a sync and a verification make their requests from many threads, with and without the GIL
    usage = S3Usage()
    thread_count = 8
    records_per_thread = 2000

    def _record():
        for _unused_index in range(records_per_thread):
            usage.record("GetObject", download_size=2)

    threads = [Thread(target=_record) for _unused_index in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert usage.get_request_count("GetObject") == thread_count * records_per_thread
    assert usage.get_download_size() == 2 * thread_count * records_per_thread


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_usage_of_a_sync_and_a_verification(tmp_path):
    prefix = f"{usage_prefix}sync/"
    contents = {"a.txt": "a", "sub/b.txt": "bb", "c.txt": "ccc"}
    _write(_make_s3_access(), prefix, contents)  # written by another S3Access, so the writes are not in the usage below
    total_size = sum(len(content) for content in contents.values())

    s3_access = _make_s3_access()
    assert s3_access.usage.get_request_count() == 0  # making an S3Access asks S3 nothing that is charged for

    # a sync lists the bucket, and makes a metadata request and a download request per downloaded object
    sync_result = s3_access.sync_to_directory(tmp_path, prefix)
    assert sync_result.get_count(S3SyncOutcome.downloaded) == len(contents)
    usage = s3_access.usage
    assert usage.get_request_count("ListObjectsV2") == 1
    assert usage.get_request_count("HeadObject") == len(contents)
    assert usage.get_request_count("GetObject") == len(contents)
    assert usage.get_download_sizes() == {standard_storage_class: total_size}
    list_count = usage.get_tier_counts()[(S3RequestTier.tier1, standard_storage_class)]
    assert list_count == 1

    # a sync that has nothing to download only lists the bucket
    usage.reset()
    sync_result = s3_access.sync_to_directory(tmp_path, prefix)
    assert sync_result.get_count(S3SyncOutcome.up_to_date) == len(contents)
    assert usage.get_request_count("ListObjectsV2") == 1
    assert usage.get_request_count("HeadObject") == usage.get_request_count("GetObject") == 0
    assert usage.get_download_size() == 0

    # a verification with the result of the sync makes a metadata request per object, and nothing else
    usage.reset()
    verify_result = s3_access.verify_directory(tmp_path, prefix, sync_result=sync_result)
    assert verify_result.get_count(S3VerifyOutcome.verified) == len(contents)
    assert usage.get_request_counts() == {("HeadObject", standard_storage_class): len(contents)}
    assert usage.get_tier_counts() == {(S3RequestTier.tier2, standard_storage_class): len(contents)}
    assert usage.get_download_size() == 0


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_usage_has_the_storage_class_of_the_object(tmp_path):
    # AWS charges more for a request for an object of e.g. STANDARD_IA or GLACIER_IR, so the requests are counted by the storage class of their object
    prefix = f"{usage_prefix}storage_class/"
    writer = _make_s3_access()
    _write(writer, prefix, {"standard.txt": "standard"})
    infrequent_content = b"infrequent access"
    infrequent_key = f"{prefix}infrequent.txt"
    writer.client.put_object(Bucket=test_awsimple_str, Key=infrequent_key, Body=infrequent_content, StorageClass=infrequent_access, ChecksumAlgorithm="CRC64NVME")
    assert writer.usage.get_request_count("PutObject") == 2  # a request the caller makes with the client is counted too

    s3_access = _make_s3_access()
    sync_result = s3_access.sync_to_directory(tmp_path, prefix)
    assert sync_result.get_count(S3SyncOutcome.downloaded) == 2
    usage = s3_access.usage
    assert usage.get_request_count("HeadObject", infrequent_access) == 1
    assert usage.get_request_count("GetObject", infrequent_access) == 1
    assert usage.get_request_count("HeadObject", standard_storage_class) == 1
    assert usage.get_request_count("GetObject", standard_storage_class) == 1
    assert usage.get_download_sizes() == {infrequent_access: len(infrequent_content), standard_storage_class: len("standard")}
    assert usage.get_tier_counts()[(S3RequestTier.tier2, infrequent_access)] == 2

    usage.reset()
    s3_access.verify_directory(tmp_path, prefix, sync_result=sync_result)
    assert usage.get_request_counts() == {("HeadObject", infrequent_access): 1, ("HeadObject", standard_storage_class): 1}


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_usage_counts_reads_writes_and_deletes():
    # the requests through the boto3 resource (read, delete) are counted as those through the client are
    s3_access = _make_s3_access()
    s3_key = f"{usage_prefix}read_write.txt"
    content = "read and written"
    s3_access.write_string(content, s3_key)
    usage = s3_access.usage
    assert usage.get_request_count("PutObject") == 1
    assert usage.get_download_size() == 0  # an upload is not a download

    usage.reset()
    assert s3_access.read_string(s3_key) == content
    assert usage.get_request_count("GetObject") == 1
    assert usage.get_download_size() == len(content)

    usage.reset()
    s3_access.delete_object(s3_key)
    assert usage.get_request_count("DeleteObject") == 1
    assert usage.get_tier_counts() == {(S3RequestTier.free, standard_storage_class): 1}


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_usage_counts_a_request_that_s3_refuses():
    # AWS charges for a request it answers with an error, e.g. the metadata of an object that does not exist
    s3_access = _make_s3_access()
    with pytest.raises(ClientError):
        s3_access.client.head_object(Bucket=test_awsimple_str, Key=f"{usage_prefix}there_is_no_such_object")
    with pytest.raises(ClientError):
        s3_access.client.get_object(Bucket=test_awsimple_str, Key=f"{usage_prefix}there_is_no_such_object")
    assert s3_access.usage.get_request_counts() == {("HeadObject", standard_storage_class): 1, ("GetObject", standard_storage_class): 1}
    assert s3_access.usage.get_download_size() == 0


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_usage_counts_a_bucket_listing_and_bucket_exists():
    s3_access = _make_s3_access()
    assert s3_access.bucket_exists()
    assert s3_access.usage.get_request_count("HeadBucket") == 1  # also with real AWS, where this request is made with a client of its own
    s3_access.usage.reset()
    s3_access.list_objects(f"{usage_prefix}nothing_has_this_prefix/")
    assert s3_access.usage.get_request_count("ListObjectsV2") == 1
    assert s3_access.usage.get_tier_counts()[(S3RequestTier.tier1, standard_storage_class)] == 1
