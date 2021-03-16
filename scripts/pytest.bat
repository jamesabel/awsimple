REM run pytest with and without mocking
pushd .
cd ..
call venv\Scripts\activate.bat
set PYTHONPATH=%CD%
set AWSIMPLE_USE_MOTO_MOCK=1
python -m pytest -s test_awsimple --cov-report xml:coverage.xml --cov-report html --cov=.\awsimple
set AWSIMPLE_USE_MOTO_MOCK=0
python -m pytest
set PYTHONPATH=
popd
