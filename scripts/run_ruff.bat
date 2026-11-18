pushd .
cd ..
call venv\Scriptsctivate.bat
ruff format .
ruff check .
call deactivate
popd
