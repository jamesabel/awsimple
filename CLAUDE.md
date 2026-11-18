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
- Custom exceptions in `exceptions.py`: `DynamoDBItemAlreadyExists`, `S3BucketAlreadyExistsNotOwnedByYou`
