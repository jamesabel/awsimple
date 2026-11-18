# Makes the development environment in .venv with uv (https://docs.astral.sh/uv/), which also installs Python if needed.
# --upgrade: the newest versions of the dependencies, not the ones in a uv.lock left from an earlier run
uv sync --upgrade
# run the tests on all supported Python versions before each commit (see .pre-commit-config.yaml and tox.ini)
uv run pre-commit install
