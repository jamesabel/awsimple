# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**awsimple** is a Python library providing simplified APIs for AWS services (S3, DynamoDB, SNS, SQS, CloudWatch Logs). It wraps boto3 with sensible defaults, local caching, and type safety via `typeguard`.

## Common Commands

```bash
# Make the dev environment (.venv) with uv: awsimple, its dependencies and the dev group in pyproject.toml.
# The dev environment is free-threaded Python (3.14t), which is what make_venv_dev.bat / make_venv_dev.sh make.
uv sync --python 3.14t

# Run all tests (uses moto mock by default via conftest.py)
uv run pytest test_awsimple

# Run all tests on every supported Python version (also runs as a pre-commit hook, takes several minutes)
uv run tox

# Run a single test file
uv run pytest test_awsimple/test_s3_write_read.py

# Run a single test
uv run pytest test_awsimple/test_s3_write_read.py::test_s3_write_read

# Lint
uv run ruff check .

# Type checking
uv run ty check awsimple

# Format
uv run ruff format .
```

## Architecture

All service classes inherit from `AWSAccess` (aws.py), which manages boto3 sessions, credentials, and mock detection:

```
AWSAccess (aws.py)
├── CacheAccess (cache.py) - adds LRU disk caching
│   ├── S3Access (s3.py) - S3 with SHA512 verification and local caching
│   └── DynamoDBAccess (dynamodb.py) - DynamoDB with scan caching via metadata table
│       └── DynamoDBMIVUI (dynamodb_miv.py) - DynamoDB with monotonic timestamps
├── SQSAccess / SQSPollAccess (sqs.py) - SQS with auto visibility timeout
├── SNSAccess (sns.py) - SNS topics
└── LogsAccess (logs.py) - CloudWatch Logs
```

The S3 code is split by concern. `s3.py` has `S3Access`, which does every access to S3; the modules it uses do not access S3:

- `s3_object.py`: `S3ObjectMetadata`, `S3ListedObject` and the hash rules of an S3 object
- `s3_paths.py`: `s3_key_to_local_path()`
- `s3_cache.py`: `S3ContentCache`, the local cache of S3 object contents (on top of the LRU cache in `cache.py`)
- `s3_sync.py`: the plan (`plan_sync()`) and result types of `S3Access.sync_to_directory()`

**Pub/Sub** (pubsub.py): High-level `Pub` and `Sub` classes that compose SNS+SQS into a channel-based messaging abstraction with background polling threads.

## Testing Modes

Controlled by environment variables (auto-set in conftest.py):
- **Moto mock** (default in CI): `AWSIMPLE_USE_MOTO_MOCK=1` — fast, no AWS needed
- **Real AWS**: `AWSIMPLE_USE_MOTO_MOCK=0` — requires AWS credentials. The real test bucket keeps its objects between runs (moto starts empty), and real S3 refuses some requests that moto takes, so a change to S3 behavior is also run this way

## Code Style

- **ruff** for formatting and linting, with line-length=192 and max-complexity=10 (pyproject.toml)
- **ty** for type checking
- CI fails on any ruff or ty finding
- All public functions use `@typechecked()` decorator from typeguard
- Python >=3.12 required
- Free-threaded Python (no GIL, e.g. 3.14t) is supported and is in the CI matrix and tox envlist. Shared state must be safe without the GIL (use a lock, as `_moto_state` in `aws.py` does), and a new dependency must not turn the GIL back on (`test_free_threading.py` checks). An `S3Access` instance has per-instance state (`download_status`), so use one per thread

## Key Patterns

- Dependencies are managed with **uv**: runtime dependencies and the `dev` and `docs` dependency groups are in `pyproject.toml`. `uv.lock` is git-ignored on purpose, so CI and tox install the newest versions
- Packaging metadata is in `pyproject.toml` (hatchling); the version is defined in `awsimple/__version__.py` and read from there at build time
- Public API is exported from `awsimple/__init__.py`
- DynamoDB uses a hidden metadata table to track modification times for cache invalidation
- The mock detection function (`is_mock()`) is in `mock.py`
- Custom exceptions in `exceptions.py`: `DynamoDBItemAlreadyExists`, `S3BucketAlreadyExistsNotOwnedByYou`
- Naming: nothing has "legacy" in its name (owner's rule). Name a thing for what it is, not for its history
- 9.1.0 is published to PyPI (2026-11-22; 9.0.0 was never published), so from 9.1.0 on, 9.x stays compatible with earlier 9.x: a change for existing code needs a new major version, or a deprecation first
- Releases: bump `awsimple/__version__.py` in a PR, then publish a GitHub release whose tag is the version (e.g. `v9.1.0`); `.github/workflows/publish.yml` builds the wheel and uploads it to PyPI through a Trusted Publisher, with Provenance. Never upload from a workstation, and only release when the owner asks for it in that request
- Do not repeat a pattern: use the common helper, or make one. In `S3Access`: `_retry()` (every retried transfer), `_list_contents()` (every bucket listing), `_write_object()`, `_is_upload_needed()` and `_finish_download()` (the check and mtime of every download). In `DynamoDBAccess`: `_get_table()`, `_table_not_found()`, `_table_modified()` (after every write) and `_get_item_key()`. In `AWSAccess`: `_create_client_and_resource()`
- Every module, class and function has a docstring (exception `__init__`/`__str__` and local closures excepted)
- Cache writes are atomic: write to a temporary file in the same directory, then move it over the destination in one step, with `atomic_write_path()` in `cache.py`. Never write a cache file in place

## S3 Hash and Integrity Rules

These are requirements from the owner. Keep `awsimple/s3.py` and its tests (`test_awsimple/test_s3_cache_key.py`, `test_s3_cache_hits.py`, `test_s3_file_based_hashes.py`) in line with them.

- **File-based hashes:** awsimple's hashes are hashes of the whole file. awsimple does not concern itself with whether a file is uploaded or downloaded in one part or in several: the hashes it writes and compares must be the same either way. Never choose a hash or a checksum algorithm by transfer size or method.
- **The SHA-512 is definitive:** the SHA-512 of an S3 object (native, or the `awsimple-sha512` object metadata, `S3ObjectMetadata.metadata_sha512`) definitively represents the contents. It is the cache key as it is, with no local record needed, so S3 objects with the same contents are one cache file.
- **Writes:** every S3 object written by awsimple also gets the SHA-512 of its contents, as the `awsimple-sha512` object metadata (an S3 object has one native checksum, and awsimple's is CRC64NVME). An upload is only skipped when the S3 object has the CRC64NVME, the length and the SHA-512 of the contents: an object that lacks the native checksum or the SHA-512 is written again, so that it gains it.
- **Reads:** if an S3 object has a SHA-512 (S3's native full-object SHA-512, or the `awsimple-sha512` object metadata), it is used. S3 does not validate object metadata, so a metadata SHA-512 must be a well-formed SHA-512, and downloaded contents are only cached once their calculated SHA-512 matches it.
- **No SHA-512:** awsimple never updates an S3 object's metadata on a read (S3 metadata can only be changed by copying the object onto itself, which changes its modification time and version and needs write permission). Instead, an object without a SHA-512 is checked against its native CRC64NVME, object key, object/file size and modification time (mtime): these, with the bucket, version and ETag, make `S3ObjectMetadata.get_identity_sha512()`, which finds the SHA-512 that awsimple calculated when it downloaded the contents (the `index` directory of the cache). A CRC alone is never a cache key, since a CRC is not collision resistant.
- **Object keys and the file system:** take OS file system path irregularities into account wherever a key is checked or stored locally. In the cache the key is only ever hashed, never used as a file name, so leading or doubled slashes, case-insensitive or reserved names and characters that are not allowed in a file name can not make two keys the same or make a key unusable. Everywhere else a key becomes a local path (e.g. a download to a directory), it goes through `s3_key_to_local_path()`, which keeps the path under the destination directory. Never join a directory and a raw key.
- **mtime:** a file that awsimple downloads, or copies from the cache, gets the modification time of the S3 object it was read as.
- **Caching:** S3 caching is always based on the SHA-512 of the object's contents: cached contents are in a file named with their SHA-512. A cache file is only used if it has the length of the S3 object and the SHA-512 it is named with; a cache hit sets the file's access time (eviction is of the least recently used file, and the OS does not reliably update access times); a cache file that is gone by the time it is read is a cache miss, not an error. The `index` directory is only for objects that have no SHA-512, and its files count towards the cache's maximum size.
- **Sync:** `S3Access.sync_to_directory()` decides what to download from the bucket listing alone, then makes one metadata request per downloaded object (never per listed object) for the length and CRC64NVME check. A download only replaces the local file once it is checked. Keys that can not all be local files are skipped and reported, never resolved by picking one.
- **Integrity:** every S3 access checks the length and the CRC64NVME (when the object has one) of what was transferred, after writes and after reads and downloads, and raises `AWSimpleException` on a mismatch.
