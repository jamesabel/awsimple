python3 -m venv --clear venv
source ./venv/bin/activate
python -m pip install -U -r requirements-dev.txt
# run the tests on all supported Python versions before each commit (see .pre-commit-config.yaml and tox.ini)
pre-commit install
deactivate
