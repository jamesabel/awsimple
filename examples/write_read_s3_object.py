from awsimple import S3Access
from os import getlogin

# the S3 key is the name of the object in the S3 bucket, somewhat analogous to a file name
s3_key = "hello.txt"

# setup the s3_access object
s3_access = S3Access(f"awsimple-test-bucket-{getlogin()}")  # bucket names are globally unique, so change this bucket name to something unique to you


# let's first make sure the bucket exists
s3_access.create_bucket()

# write our message to S3
s3_access.write_string("hello world", s3_key)


# will output "hello world"
print(s3_access.read_string(s3_key))
