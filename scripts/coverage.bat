pushd .
cd ..
set PYTHONPATH=%CD%
set AWSIMPLE_USE_MOTO_MOCK=1
mkdir cov
venv\Scripts\pytest.exe --cov-report=html --cov-report=xml:cov\coverage.xml --cov --ignore=examples
set PYTHONPATH=
set AWSIMPLE_USE_MOTO_MOCK=
popd
