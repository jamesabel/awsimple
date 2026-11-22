# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**awsimple** is a Python library providing simplified APIs for AWS services (S3, DynamoDB, SNS, SQS, CloudWatch Logs). It wraps boto3 with sensible defaults, local caching, and type safety via `typeguard`.

## Common Commands

```bash
# Make the dev environment (.venv) with uv: awsimple, its dependencies and the dev group in pyproject.toml
uv sync

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

**Pub/Sub** (pubsub.py): High-level `Pub` and `Sub` classes that compose SNS+SQS into a channel-based messaging abstraction with background polling threads.

## Testing Modes

Controlled by environment variables (auto-set in conftest.py):
- **Moto mock** (default in CI): `AWSIMPLE_USE_MOTO_MOCK=1` — fast, no AWS needed
- **Real AWS**: unset both env vars — requires AWS credentials
- **LocalStack**: `AWSIMPLE_USE_LOCALSTACK=1` — requires running LocalStack

## Code Style

- **ruff** for formatting and linting, with line-length=192 and max-complexity=10 (pyproject.toml)
- **ty** for type checking
- CI fails on any ruff or ty finding
- All public functions use `@typechecked()` decorator from typeguard
- Python >=3.12 required

## Key Patterns

- Dependencies are managed with **uv**: runtime dependencies and the `dev` and `docs` dependency groups are in `pyproject.toml`. `uv.lock` is git-ignored on purpose, so CI and tox install the newest versions
- Packaging metadata is in `pyproject.toml` (hatchling); the version is defined in `awsimple/__version__.py` and read from there at build time
- Public API is exported from `awsimple/__init__.py`
- DynamoDB uses a hidden metadata table to track modification times for cache invalidation
- Mock detection functions (`is_mock()`, `is_using_localstack()`) are in `mock.py`
- Cache writes are atomic: write to a temporary file in the same directory, then move it over the destination in one step, with `atomic_write_path()` in `cache.py`. Never write a cache file in place

## S3 Hash and Integrity Rules

These are requirements from the owner. Keep `awsimple/s3.py` and its tests (`test_awsimple/test_s3_cache_key.py`) in line with them.

- **Writes:** every S3 object written by awsimple also gets the SHA-512 of its contents, as the `awsimple-sha512` object metadata (an S3 object has one native checksum, and awsimple's is CRC64NVME).
- **Reads:** if an S3 object has a SHA-512 (S3's native full-object SHA-512, or the `awsimple-sha512` object metadata), it is used. S3 does not validate object metadata, so a metadata SHA-512 must be a well-formed SHA-512, and downloaded contents are only cached once their calculated SHA-512 matches it.
- **No SHA-512:** awsimple never updates an S3 object's metadata on a read (S3 metadata can only be changed by copying the object onto itself, which changes its modification time and version and needs write permission). Instead, an object without a SHA-512 is checked against its native CRC64NVME, object key, object/file size and modification time (mtime): these, with the bucket, version and ETag, make `S3ObjectMetadata.get_identity_sha512()`, which finds the SHA-512 that awsimple calculated when it downloaded the contents (the `index` directory of the cache). A CRC alone is never a cache key, since a CRC is not collision resistant.
- **Object keys and the file system:** take OS file system path irregularities into account wherever a key is checked or stored locally. In the cache the key is only ever hashed, never used as a file name, so leading or doubled slashes, case-insensitive or reserved names and characters that are not allowed in a file name can not make two keys the same or make a key unusable. Everywhere else a key becomes a local path (e.g. a download to a directory), it goes through `s3_key_to_local_path()`, which keeps the path under the destination directory. Never join a directory and a raw key.
- **mtime:** a file that awsimple downloads, or copies from the cache, gets the modification time of the S3 object it was read as.
- **Caching:** S3 caching is always based on the SHA-512 of the object's contents: cached contents are in a file named with their SHA-512.
- **Sync:** `S3Access.sync_to_directory()` decides what to download from the bucket listing alone, then makes one metadata request per downloaded object (never per listed object) for the length and CRC64NVME check. A download only replaces the local file once it is checked. Keys that can not all be local files are skipped and reported, never resolved by picking one.
- **Integrity:** every S3 access checks the length and the CRC64NVME (when the object has one) of what was transferred, after writes and after reads and downloads, and raises `AWSimpleException` on a mismatch.
- Custom exceptions in `exceptions.py`: `DynamoDBItemAlreadyExists`, `S3BucketAlreadyExistsNotOwnedByYou`
