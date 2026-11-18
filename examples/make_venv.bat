rmdir /S /Q venv
uv venv --python 3.15 venv
uv pip install --python venv -U -r requirements-examples.txt
