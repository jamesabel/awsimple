pushd .
cd ..
call venv\Scripts\activate.bat 
mypy -m awsimple
mypy -m test_awsimple
call deactivate
popd
