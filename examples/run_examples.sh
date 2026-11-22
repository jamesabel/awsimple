source venv/bin/activate
python -m aws_access_test
python -m write_read_s3_object
python -m read_s3_object
python -m derived_access_class
python -m dynamodb_partition_only
python -m dynamodb_partition_and_sort
# the S3 cache demonstration with the moto mock (see EXAMPLES.md to run it with a real bucket)
python -m s3_cache_demo --mock
deactivate
