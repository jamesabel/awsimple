REM run pytest with the moto mock
pushd .
cd ..
uv run pytest -s test_awsimple --cov-report xml:coverage.xml --cov-report html --cov=.\awsimple
popd
