REM Makes the development environment in .venv with uv (https://docs.astral.sh/uv/), which also installs Python if needed.
REM The environment is free-threaded Python (no GIL): 3.14t. Not 3.15t yet, since boto3[crt] pins an awscrt version that has no
REM free-threaded 3.15 wheel. The other supported Python versions are tested by tox (see tox.ini).
rmdir /S /Q .venv
REM --upgrade: the newest versions of the dependencies, not the ones in a uv.lock left from an earlier run
uv sync --upgrade --python 3.14t
REM run the tests on all supported Python versions before each commit (see .pre-commit-config.yaml and tox.ini)
uv run pre-commit install
