# free-threaded Python (no GIL), as the development environment is
uv venv --clear --python 3.14t venv
uv pip install --python venv -U -r requirements-examples.txt
