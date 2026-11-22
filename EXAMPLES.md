# Running the examples for awsimple

The examples are in the `examples` folder, and the commands below are run from that folder (`cd examples`).
[uv](https://docs.astral.sh/uv/) is required to make the virtual environment. When run, the examples
 - check that you have access to AWS (`aws_access_test.py`)
 - write an object called "hello.txt" to the S3 bucket awsimple-test-bucket-{your login name}, and read it back (`write_read_s3_object.py`)
     - Note: It is strongly recommended to change the bucket name before you run this, but it will work without it
 - do the same with a class derived from `S3Access` (`derived_access_class.py`)
 - put and get DynamoDB items (`dynamodb_partition_only.py` and `dynamodb_partition_and_sort.py`, run by `run_examples.bat` only)

`read_s3_object.py` and the `pubsub` folder are further examples that the run scripts do not run.

### The S3 cache demonstration

`s3_cache_demo.py` shows how the S3 cache works and measures what it saves. It downloads the same S3 objects with and
without the cache, counts the requests to S3 and the bytes downloaded, and times each step. The download destination
and the cache are temporary directories that it leaves in place and prints, so you can look at how the cache is made.

```
python s3_cache_demo.py --mock                # no AWS account needed (moto mock): the counts, but not meaningful times
python s3_cache_demo.py --bucket my-bucket    # real AWS: the default profile, or add --profile
```

The S3 objects it writes have keys that start with `s3_cache_demo/` and are deleted at the end (unless `--keep`).



### 1. Make the Virtual Environment (the run scripts activate it)

#### Mac / Linux
```
source make_venv.sh
```

#### Windows
```
make_venv.bat
```

### 2. Check your AWS profile and create a test user name "testawsimple" with read/write access to s3.

Your default aws profile should be setup before you run the examples. The examples use a test user named "testawsimple". You should create this user before running the examples.

```
aws configure
```

### 3. Run the examples

#### Mac / Linux
```
source run_examples.sh
```

#### Windows
```
run_examples.bat
```



### Got a problem?

You're welcome to [create an issue](https://github.com/jamesabel/awsimple/issues/new), but please [search existing ones](https://github.com/jamesabel/awsimple/issues) first to see if it's been discussed before.
