pushd .
cd ..
set PYTHONPATH=%CD%
venv\Scripts\pytest.exe --cov-report=html --cov
set PYTHONPATH=
popd
