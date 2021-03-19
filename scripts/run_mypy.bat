pushd .
cd ..
call venv\Scripts\activate.bat 
mypy -m awsimple
call deactivate
popd
