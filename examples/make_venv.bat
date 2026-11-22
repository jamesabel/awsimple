rmdir /S /Q venv
REM free-threaded Python (no GIL), as the development environment is
uv venv --python 3.14t venv
uv pip install --python venv -U -r requirements-examples.txt
