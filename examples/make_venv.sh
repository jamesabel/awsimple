rm -rf venv
python3.8 -m venv --clear venv
./venv/bin/python -m pip install --upgrade pip
./venv/bin/pip3 install -U setuptools
./venv/bin/pip3 install -U -r requirements-examples.txt