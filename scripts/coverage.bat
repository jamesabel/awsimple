pushd .
cd ..
set PYTHONPATH=%CD%
set AWSIMPLE_USE_MOTO_MOCK=0
mkdir cov
venv\Scripts\pytest.exe --cov-report=html --cov-report=xml:cov\coverage.xml --cov --ignore=examples
venv\Scripts\python.exe scripts\doc_coverage_updater.py
set PYTHONPATH=
set AWSIMPLE_USE_MOTO_MOCK=
popd
