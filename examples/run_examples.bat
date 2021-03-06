call venv\Scripts\activate.bat
python -m aws_access_test
python -m write_read_s3_object
python -m derived_access_class
python -m dynamodb_partition_only
python -m dynamodb_partition_and_sort
deactivate
