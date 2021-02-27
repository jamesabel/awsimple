REM run pytest with and without mocking
pushd .
cd ..
call venv\Scripts\activate.bat
set PYTHONPATH=%CD%
set AWSIMPLE_USE_MOTO_MOCK=1
python -m pytest
set AWSIMPLE_USE_MOTO_MOCK=0
python -m pytest
set PYTHONPATH=
popd
