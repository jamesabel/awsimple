pushd .
cd ..
set AWSIMPLE_USE_MOTO_MOCK=1
mkdir cov
uv run pytest --cov-report=html --cov-report=xml:cov\coverage.xml --cov --ignore=examples
uv run python scripts\doc_coverage_updater.py
set AWSIMPLE_USE_MOTO_MOCK=
popd
