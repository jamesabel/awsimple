pushd .
cd ..
call venv\Scripts\activate.bat
python -m black -l 192 awsimple test_awsimple setup.py examples
deactivate
popd
