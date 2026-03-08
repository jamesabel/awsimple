# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**awsimple** is a Python library providing simplified APIs for AWS services (S3, DynamoDB, SNS, SQS, CloudWatch Logs). It wraps boto3 with sensible defaults, local caching, and type safety via `typeguard`.

## Common Commands

```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Run all tests (uses moto mock by default via conftest.py)
pytest test_awsimple

# Run a single test file
pytest test_awsimple/test_s3_write_read.py

# Run a single test
pytest test_awsimple/test_s3_write_read.py::test_s3_write_read

# Lint
flake8 --max-line-length=127 --max-complexity=10

# Type checking
mypy awsimple

# Format
black awsimple
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

- **black** formatting with line-length=192 (pyproject.toml)
- **flake8** with max-line-length=127, max-complexity=10
- All public functions use `@typechecked()` decorator from typeguard
- Python >=3.10 required

## Key Patterns

- Version is defined in `awsimple/__version__.py` and read by `setup.py`
- Public API is exported from `awsimple/__init__.py`
- DynamoDB uses a hidden metadata table to track modification times for cache invalidation
- Mock detection functions (`is_mock()`, `is_using_localstack()`) are in `mock.py`
- Custom exceptions in `exceptions.py`: `DynamoDBItemAlreadyExists`, `S3BucketAlreadyExistsNotOwnedByYou`
