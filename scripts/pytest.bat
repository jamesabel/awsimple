REM run pytest with and without mocking
pushd .
cd ..
call venv\Scripts\activate.bat
set PYTHONPATH=%CD%
python -m pytest -s test_awsimple --cov-report xml:coverage.xml --cov-report html --cov=.\awsimple
REM
REM set AWSIMPLE_USE_MOTO_MOCK=0
REM python -m pytest
REM
set PYTHONPATH=
set AWSIMPLE_USE_MOTO_MOCK=
popd
