"""
The HTTP connection pool of an S3Access is large enough for the requests a sync makes at the same time, so that no connection is opened only to be discarded
(urllib3's "Connection pool is full, discarding connection" warning).

All tests, helpers and constants in this file are AI-generated (Claude Code).
"""

import logging
from pathlib import Path

from boto3.s3.transfer import TransferConfig

from awsimple import S3Access, S3SyncOutcome, SQSAccess, is_mock
from awsimple.s3 import s3_max_pool_connections
from test_awsimple import test_awsimple_str

pool_prefix = "connection_pool/"
object_count = 20  # twice the number of objects a sync works on at the same time
part_size = 64 * 1024
parts_per_object = 16  # more than the number of requests the transfer manager makes at the same time


class _S3AccessSmallParts(S3Access):
    """
    Downloads in parts of 64 KiB (with the default number of concurrent requests), so that a small S3 object is downloaded in parts as a large one is by default.
    """

    def get_s3_download_transfer_config(self) -> TransferConfig:
        return TransferConfig(multipart_threshold=part_size, multipart_chunksize=part_size)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_connection_pool_covers_the_requests_of_a_sync(s3_access):
    # A sync can have a metadata request in each of its worker threads and a download request in each of the transfer manager's request threads.
    worker_count = s3_access._get_download_worker_count()
    request_thread_count = s3_access.get_s3_download_transfer_config().max_request_concurrency
    assert s3_max_pool_connections >= worker_count + request_thread_count

    config = s3_access._get_config()
    assert config.max_pool_connections == s3_max_pool_connections
    assert config.connect_timeout == config.read_timeout == 60 * 60  # the timeouts of every AWSAccess are kept

    if not is_mock():
        # the mock's client and resource are made without this configuration (they make no connections)
        assert s3_access.client.meta.config.max_pool_connections == s3_max_pool_connections
        assert s3_access.resource.meta.client.meta.config.max_pool_connections == s3_max_pool_connections


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_connection_pool_of_other_services_is_unchanged():
    assert SQSAccess(test_awsimple_str)._get_config().max_pool_connections == 10  # botocore's default


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_sync_to_directory_does_not_overflow_the_connection_pool(tmp_path, caplog):
    # With the moto mock no connection is made, so this can only fail with real AWS (AWSIMPLE_USE_MOTO_MOCK=0).
    # An object that is downloaded in parts can have a request in every request thread of the transfer manager, while the other workers of the sync make their
    # metadata requests.
    s3_access = _S3AccessSmallParts(profile_name=test_awsimple_str, bucket_name=test_awsimple_str)  # its own instance, so its own connection pool
    contents = {f"{pool_prefix}{index}.txt": chr(ord("a") + index) * (parts_per_object * part_size) for index in range(object_count)}
    for s3_key, content in contents.items():
        s3_access.write_string(content, s3_key)

    dest_dir = Path(tmp_path, "dest")
    with caplog.at_level(logging.WARNING, logger="urllib3.connectionpool"):
        result = s3_access.sync_to_directory(dest_dir, pool_prefix)
    assert result.get_count(S3SyncOutcome.downloaded) == object_count
    for s3_key, content in contents.items():
        assert Path(dest_dir, s3_key[len(pool_prefix) :]).read_text() == content
    assert [record.getMessage() for record in caplog.records if "Connection pool is full" in record.getMessage()] == []
