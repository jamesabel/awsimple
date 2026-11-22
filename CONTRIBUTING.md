# Contributing to awsimple

Hi! First of all, thank you for contributing. :heart:

All of the usual sorts of contributions are welcome: bug reports, patches, and feedback.
Feel free to [browse existing issues](https://github.com/jamesabel/awsimple/issues) or [create a new one](https://github.com/jamesabel/awsimple/issues/new).


## Got a problem?

You're welcome to [create an issue](https://github.com/jamesabel/awsimple/issues/new), but please [search existing ones](https://github.com/jamesabel/awsimple/issues) first to see if it's been discussed before.


## Want to submit some code or docs?

Great! 

If you're interested in tackling an [existing issue](https://github.com/jamesabel/awsimple/issues), comment on one to make sure you're on the right track.
If it's an idea you have or a problem not captured in an issue, [create one](https://github.com/jamesabel/awsimple/issues/new) and let's align.


### Dev setup

Requirements:

- [uv](https://docs.astral.sh/uv/), which installs Python and the dependencies

Fork the repo, to make your own personal copy, and then git clone your fork onto your computer.

Once you have that, install project dependencies with:

#### Mac / Linux
```
source make_venv_dev.sh
```

#### Windows
```
make_venv_dev.bat
```

This makes the environment in `.venv`, with free-threaded Python (3.14t, the build without the GIL). `awsimple` supports Python 3.12 and higher, with or
without the GIL; `uv run tox` runs the tests on every supported version. At this point you should be able to make changes to the codebase and run things,
e.g. the tests with `uv run pytest test_awsimple`.

### Running the tests

The tests use the [moto](https://github.com/getmoto/moto) mock of AWS unless told otherwise, so they need no AWS account:

```
uv run pytest test_awsimple
```

To run them with real AWS, set `AWSIMPLE_USE_MOTO_MOCK=0`. This needs an AWS profile named `testawsimple` (in `~/.aws/credentials` and
`~/.aws/config`, with a region) that is allowed to use S3, DynamoDB, SNS, SQS and CloudWatch Logs, and it makes AWS resources in that account
(e.g. the S3 bucket and DynamoDB tables named `testawsimple`), which may cost money. It takes several times as long as the mock.

#### Mac / Linux
```
AWSIMPLE_USE_MOTO_MOCK=0 uv run pytest test_awsimple
```

#### Windows
```
set AWSIMPLE_USE_MOTO_MOCK=0
uv run pytest test_awsimple
```

A change to how `awsimple` accesses AWS should pass both ways. The mock is not AWS: e.g. S3 refuses some object keys that moto accepts. A
real bucket also keeps its objects from one run to the next (the mock starts empty), so a test must not depend on a bucket or a key prefix
being empty, and must pass when it is run again.
