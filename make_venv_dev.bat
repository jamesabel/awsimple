rmdir /S /Q venv
c:"\Program Files\Python315\python.exe" -m venv --clear venv
venv\Scripts\python.exe -m pip install --upgrade pip
REM PyYAML (needed by moto and awscli) has no Python 3.15 wheel yet, so pip builds it from source. Its optional C
REM extension needs MSVC, so build the pure Python version instead. Remove this once PyYAML ships a cp315 wheel.
set PYYAML_FORCE_LIBYAML=0
venv\Scripts\pip3 install -U -r requirements-dev.txt
set PYYAML_FORCE_LIBYAML=
REM run the tests on all supported Python versions before each commit (see .pre-commit-config.yaml and tox.ini)
venv\Scripts\pre-commit install
