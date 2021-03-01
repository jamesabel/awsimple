pushd .
cd ..
call venv\Scripts\activate.bat
sphinx-build -M html doc_source build
call deactivate
popd
