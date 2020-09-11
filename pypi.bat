rmdir /S /Q awsimple.egg-info
rmdir /S /Q build
rmdir /S /Q dist
copy /Y LICENSE LICENSE.txt
call venv\Scripts\activate.bat
python.exe setup.py bdist_wheel
twine upload dist/*
rmdir /S /Q awsimple.egg-info
rmdir /S /Q build
deactivate
