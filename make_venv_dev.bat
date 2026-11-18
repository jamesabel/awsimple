REM Makes the development environment in .venv with uv (https://docs.astral.sh/uv/), which also installs Python if needed.
rmdir /S /Q .venv
REM PyYAML (needed by moto and awscli) has no Python 3.15 wheel yet, so it is built from source. Its optional C
REM extension needs MSVC, so build the pure Python version instead. Remove this once PyYAML ships a cp315 wheel.
set PYYAML_FORCE_LIBYAML=0
REM --upgrade: the newest versions of the dependencies, not the ones in a uv.lock left from an earlier run
uv sync --upgrade --python 3.15
set PYYAML_FORCE_LIBYAML=
REM run the tests on all supported Python versions before each commit (see .pre-commit-config.yaml and tox.ini)
uv run pre-commit install
