"""
Error paths and less common branches of AWSAccess, lru_cache_write, dict_to_dynamodb, DynamoDBAccess.create_table, S3Access.upload
and SQSAccess receive, which the rest of the test suite does not reach.

All tests in this file are AI-generated (Claude Code); the private helpers and constants below are covered by this note.

The tests replace parts of awsimple and boto3 to force the branches, so they run with the moto mock only.
"""

import decimal
import logging
import os
import time
from pathlib import Path

import pytest
from botocore.exceptions import ClientError
from PIL import Image

from awsimple import AWSAccess, AWSimpleException, DynamoDBAccess, SQSAccess, __application_name__, dict_to_dynamodb, is_mock, lru_cache_write

from test_awsimple import test_awsimple_str

pytestmark = pytest.mark.skipif(not is_mock(), reason="replaces parts of awsimple and boto3, so only meaningful with the moto mock")


def _client_error(operation_name: str) -> ClientError:
    return ClientError({"Error": {"Code": "TestError", "Message": "forced by the test"}}, operation_name)


def _do_not_fail_on_error_log(monkeypatch) -> None:
    # the test session's logging handler (on the root logger) turns an error log into an assert
    monkeypatch.setattr(logging.getLogger(__application_name__), "propagate", False)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.parametrize("use_localstack", [True, False])
def test_aws_access_without_mock_creates_client_and_resource(monkeypatch, use_localstack):
    # no AWS call is made: this only creates the boto3 objects
    monkeypatch.setattr("awsimple.aws.is_mock", lambda: False)
    monkeypatch.setattr("awsimple.aws.is_using_localstack", lambda: use_localstack)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")

    sqs = AWSAccess("sqs", region_name="us-west-2")
    assert sqs.client is not None
    assert sqs.resource is not None

    logs = AWSAccess("logs", region_name="us-west-2")  # logs has a client but no resource
    assert logs.client is not None
    assert logs.resource is None

    session_only = AWSAccess(region_name="us-west-2")
    assert session_only.client is None
    assert session_only.resource is None
    assert session_only.session is not None

    if use_localstack:
        assert sqs.aws_access_key_id == "test"
        assert sqs.region_name == "us-west-2"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_lru_cache_write_bytes_and_file_without_limit(tmp_path):
    cache_dir = Path(tmp_path, "cache")
    assert lru_cache_write(b"abc", cache_dir, "from_bytes")
    assert Path(cache_dir, "from_bytes").read_bytes() == b"abc"

    source_path = Path(tmp_path, "source.txt")
    source_path.write_bytes(b"defg")
    assert lru_cache_write(source_path, cache_dir, "from_file")
    assert Path(cache_dir, "from_file").read_bytes() == b"defg"


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_lru_cache_write_larger_than_cache(tmp_path):
    cache_dir = Path(tmp_path, "cache")
    cache_dir.mkdir()
    assert not lru_cache_write(b"0123456789", cache_dir, "too_big", max_absolute_cache_size=5)
    assert not Path(cache_dir, "too_big").exists()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_lru_cache_write_evicts_least_recently_used(tmp_path):
    cache_dir = Path(tmp_path, "cache")
    Path(cache_dir, "sub").mkdir(parents=True)  # a directory is never evicted
    old_path = Path(cache_dir, "old")
    recent_path = Path(cache_dir, "sub", "recent")
    now = time.time()
    for file_path, access_time in ((old_path, now - 1000.0), (recent_path, now - 10.0)):
        file_path.write_bytes(b"x" * 40)
        os.utime(file_path, (access_time, access_time))

    assert lru_cache_write(b"y" * 50, cache_dir, "new", max_absolute_cache_size=100)

    assert not old_path.exists()  # evicted to make room
    assert recent_path.exists()
    assert Path(cache_dir, "new").read_bytes() == b"y" * 50


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_lru_cache_write_missing_source_file(tmp_path):
    cache_dir = Path(tmp_path, "cache")
    assert not lru_cache_write(Path(tmp_path, "does_not_exist.txt"), cache_dir, "missing")
    assert not Path(cache_dir, "missing").exists()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_dict_to_dynamodb_image():
    image = Image.new("RGB", (2, 2))
    converted = dict_to_dynamodb({"image": image})
    assert converted["image"].startswith(b"\x89PNG")

    # an image that is not converted is an unsupported type
    assert dict_to_dynamodb({"image": image}, convert_images=False, raise_exception=False) == {"image": None}
    with pytest.raises(NotImplementedError):
        dict_to_dynamodb({"image": image}, convert_images=False)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_dict_to_dynamodb_unsupported_type():
    with pytest.raises(NotImplementedError):
        dict_to_dynamodb({"value": object()})
    assert dict_to_dynamodb({"value": object()}, raise_exception=False) == {"value": None}


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_dict_to_dynamodb_number_without_inexact_handling(monkeypatch):
    monkeypatch.setattr("awsimple.dynamodb.handle_inexact_error", False)
    converted = dict_to_dynamodb({"integer": 2, "float": 0.5})
    assert converted == {"integer": decimal.Decimal(2), "float": decimal.Decimal(0.5)}
    assert all(type(value) is decimal.Decimal for value in converted.values())


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_dynamodb_create_table_key_types():
    bytes_key_table = DynamoDBAccess(f"{test_awsimple_str}_bytes_key", profile_name=test_awsimple_str)
    assert bytes_key_table.create_table("id", sort_key="sort", partition_key_type=bytes, sort_key_type=int)
    key_types = {
        definition["AttributeName"]: definition["AttributeType"] for definition in bytes_key_table.client.describe_table(TableName=bytes_key_table.table_name)["Table"]["AttributeDefinitions"]
    }
    assert key_types == {"id": "B", "sort": "N"}

    bool_key_table = DynamoDBAccess(f"{test_awsimple_str}_bool_key", profile_name=test_awsimple_str)
    with pytest.raises(ValueError, match="boolean"):
        bool_key_table.create_table("id", partition_key_type=bool)
    assert not bool_key_table.table_exists()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_dynamodb_create_table_client_error(monkeypatch):
    table = DynamoDBAccess(f"{test_awsimple_str}_create_error", profile_name=test_awsimple_str)

    def failing_create_table(**kwargs):
        raise _client_error("CreateTable")

    monkeypatch.setattr(table.client, "create_table", failing_create_table)
    assert not table.create_table("id")  # logged as a warning, not raised
    assert not table.table_exists()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_upload_path_as_string(s3_access, tmp_path):
    file_path = Path(tmp_path, "upload_as_string.txt")
    file_path.write_text("path given as a string")
    s3_access.create_bucket()
    assert s3_access.upload(str(file_path), "upload_as_string.txt", force=True)
    assert s3_access.object_exists("upload_as_string.txt")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.mark.parametrize("error", [_client_error("PutObject"), RuntimeError("forced by the test")])
def test_s3_upload_retries_after_error(s3_access, tmp_path, monkeypatch, error):
    _do_not_fail_on_error_log(monkeypatch)  # a RuntimeError during the transfer is logged as an error
    file_path = Path(tmp_path, "upload_retry.txt")
    file_path.write_text("uploaded on the second attempt")
    s3_access.create_bucket()

    original_upload_file = s3_access.client.upload_file
    attempts = []

    def upload_file_failing_once(*args, **kwargs):
        attempts.append(args)
        if len(attempts) == 1:
            raise error
        return original_upload_file(*args, **kwargs)

    monkeypatch.setattr(s3_access.client, "upload_file", upload_file_failing_once)
    monkeypatch.setattr(s3_access, "retry_sleep_time", 0.0)

    assert s3_access.upload(file_path, "upload_retry.txt", force=True)
    assert len(attempts) == 2
    assert s3_access.object_exists("upload_retry.txt")


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_s3_upload_fails_after_all_retries(s3_access, tmp_path, monkeypatch):
    file_path = Path(tmp_path, "upload_never.txt")
    file_path.write_text("never uploaded")
    s3_access.create_bucket()

    attempts = []

    def upload_file_failing(*args, **kwargs):
        attempts.append(args)
        raise _client_error("PutObject")

    monkeypatch.setattr(s3_access.client, "upload_file", upload_file_failing)
    monkeypatch.setattr(s3_access, "retry_sleep_time", 0.0)
    monkeypatch.setattr(s3_access, "retry_count", 3)

    with pytest.raises(AWSimpleException, match="3 attempts"):
        s3_access.upload(file_path, "upload_never.txt", force=True)
    assert len(attempts) == 3


def _make_history_queue(queue_name: str, history_path: Path, monkeypatch) -> SQSAccess:
    """
    A queue that keeps a response history (i.e. the user deletes the messages), with its history file at the given path.
    """
    queue = SQSAccess(queue_name, immediate_delete=False, profile_name=test_awsimple_str)
    queue.create_queue()
    monkeypatch.setattr(queue, "_get_response_history_file_path", lambda: history_path)
    return queue


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_sqs_receive_with_unreadable_history_file(tmp_path, monkeypatch):
    # not JSON
    corrupt_path = Path(tmp_path, "corrupt.json")
    corrupt_path.write_text("this is not JSON")
    corrupt_queue = _make_history_queue(f"{test_awsimple_str}_history_corrupt", corrupt_path, monkeypatch)
    assert corrupt_queue.receive_messages() == []
    assert "__initial__" in corrupt_queue.response_history  # the history starts over

    # can not be opened as a file
    directory_path = Path(tmp_path, "directory.json")
    directory_path.mkdir()
    directory_queue = _make_history_queue(f"{test_awsimple_str}_history_directory", directory_path, monkeypatch)
    assert directory_queue.receive_messages() == []
    assert "__initial__" in directory_queue.response_history


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_sqs_receive_history_is_limited(tmp_path, monkeypatch):
    queue = _make_history_queue(f"{test_awsimple_str}_history_limit", Path(tmp_path, "history.json"), monkeypatch)
    queue.max_history = 2
    for number in range(4):
        queue.send(str(number))

    messages = []  # type: list
    deadline = time.time() + 30.0
    while len(messages) < 4 and time.time() < deadline:
        messages.extend(queue.receive_messages())

    assert sorted(message.message for message in messages) == ["0", "1", "2", "3"]
    assert len(queue.response_history) == 2  # the oldest entries were dropped
    for message in messages:
        message.delete()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_sqs_receive_queue_not_available(monkeypatch):
    queue = SQSAccess(f"{test_awsimple_str}_not_available", profile_name=test_awsimple_str)
    monkeypatch.setattr(queue, "_get_queue", lambda: None)
    assert queue.receive_messages() == []
    assert queue.receive_message() is None


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_sqs_receive_client_error(monkeypatch):
    queue = SQSAccess(f"{test_awsimple_str}_client_error", profile_name=test_awsimple_str)

    def failing_get_queue():
        raise _client_error("ReceiveMessage")

    monkeypatch.setattr(queue, "_get_queue", failing_get_queue)
    assert queue.receive_messages() == []  # the error is not raised
    assert queue.most_recent_error is not None
    assert "TestError" in queue.most_recent_error
