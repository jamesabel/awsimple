"""
The one place awsimple calculates a CRC64NVME (awsimple/crc64nvme.py): every implementation gives the same value, and the fastest one that can be used is used.

All tests, fixtures, helpers and constants in this file are AI-generated (Claude Code).
"""

import builtins
import os
from pathlib import Path

import pytest

from awsimple import crc64nvme, get_bytes_crc64nvme, get_crc64nvme_implementation, get_file_crc64nvme

check_value = "ae8b14860a799888"  # the CRC64NVME of b"123456789" (the check value of the CRC-64/NVME specification)


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
@pytest.fixture
def no_awscrt(monkeypatch):
    # as if awscrt could not be imported
    real_import = builtins.__import__

    def _import(name, *args, **kwargs):
        if name == "awscrt" or name.startswith("awscrt."):
            raise ImportError("no awscrt (from the test)")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _import)
    crc64nvme.get_crc64nvme_implementation.cache_clear()
    yield
    crc64nvme.get_crc64nvme_implementation.cache_clear()


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_crc64nvme_check_value(tmp_path):
    assert get_bytes_crc64nvme(b"123456789") == check_value
    assert get_bytes_crc64nvme(b"") == "0000000000000000"
    file_path = Path(tmp_path, "check.txt")
    file_path.write_bytes(b"123456789")
    assert get_file_crc64nvme(file_path) == check_value


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_crc64nvme_awscrt_is_used():
    # awscrt is a dependency of awsimple (boto3[crt]), and is far faster than pure Python
    assert get_crc64nvme_implementation().name == "awscrt"
    assert [implementation.name for implementation in crc64nvme._get_implementations()] == ["awscrt", "hashy"]


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_crc64nvme_implementations_give_the_same_value(tmp_path, monkeypatch):
    # of bytes and of files, including an empty file and files that are read in more than one chunk (with and without a partial last chunk)
    monkeypatch.setattr(crc64nvme, "file_chunk_size", 1024)
    for size in (0, 1, 1023, 1024, 1025, 4096, 5000):
        data = os.urandom(size)
        file_path = Path(tmp_path, f"{size}.bin")
        file_path.write_bytes(data)
        values = set()
        for implementation in crc64nvme._get_implementations():
            values.add(implementation.get_bytes(data))
            values.add(implementation.get_file(file_path))
        assert len(values) == 1, (size, values)
        (value,) = values
        assert len(value) == 16 and value == value.lower()
        assert get_bytes_crc64nvme(data) == get_file_crc64nvme(file_path) == value


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_crc64nvme_without_awscrt(no_awscrt, tmp_path):
    # pure Python is used if awscrt can not be imported, with the same values
    assert get_crc64nvme_implementation().name == "hashy"
    assert get_bytes_crc64nvme(b"123456789") == check_value
    file_path = Path(tmp_path, "check.txt")
    file_path.write_bytes(b"123456789")
    assert get_file_crc64nvme(file_path) == check_value


# AI-GENERATED TEST (Claude Code) - delete this line to make this test human-owned.
def test_crc64nvme_is_only_calculated_in_one_place():
    # The rest of awsimple gets its CRC64NVME from awsimple/crc64nvme.py, so that this is where the implementation is chosen. Another module may use awscrt for
    # something else (e.g. the CRC32C in s3_verify.py), but not its CRC64NVME.
    package_dir = Path(crc64nvme.__file__).parent
    for module_path in package_dir.glob("*.py"):
        if module_path.name != "crc64nvme.py":
            source = module_path.read_text(encoding="utf-8")
            assert "checksums.crc64nvme" not in source, module_path.name  # the CRC64NVME of awscrt
            for line in source.splitlines():
                if "hashy" in line and "import" in line:
                    assert "crc64nvme" not in line, (module_path.name, line)
