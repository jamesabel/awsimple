pushd .
cd ..
uv run sphinx-build -M html doc_source build
popd
