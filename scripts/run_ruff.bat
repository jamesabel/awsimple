pushd .
cd ..
uv run ruff format .
uv run ruff check .
popd
