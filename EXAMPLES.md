# Running the examples for awsimple

The examples are in the `examples` folder, and the commands below are run from that folder (`cd examples`).
[uv](https://docs.astral.sh/uv/) is required to make the virtual environment. When run, the examples
 - check that you have access to AWS (`aws_access_test.py`)
 - write an object called "hello.txt" to the S3 bucket awsimple-test-bucket-{your user name}, and read it back (`write_read_s3_object.py`)
     - The bucket name is made in `bucket_name.py`: your user name in lower case, with any character that a bucket name can not have replaced
     - Note: It is strongly recommended to change the bucket name before you run this, but it will work without it
 - read that object again (`read_s3_object.py`)
 - do the same with a class derived from `S3Access` (`derived_access_class.py`)
 - put and get DynamoDB items (`dynamodb_partition_only.py` and `dynamodb_partition_and_sort.py`)
 - run the S3 cache demonstration with the moto mock (`s3_cache_demo.py --mock`, see below)

The `pubsub` folder has a further example that the run scripts do not run, since it is two programs that wait for you to type
(`pub_app.py` and `sub_app.py`, each in a window of its own: `pubsub.bat` starts both).

### The S3 cache demonstration

`s3_cache_demo.py` shows how the S3 cache works and measures what it saves. It downloads the same S3 objects with and
without the cache, counts the requests to S3 and the bytes downloaded, and times each step. The files it uploads, the
download destination and the cache are the `source`, `dest` and `cache` sub-directories of a data directory, which it leaves in
place and prints, so you can look at how the cache is made. The data directory is a new temporary directory, or the one you give
with `--data-dir`: nothing in it is deleted, so the program stops with a message if those sub-directories are not empty.

```
python s3_cache_demo.py --mock                # no AWS account needed (moto mock): the counts, but not meaningful times
python s3_cache_demo.py --bucket my-bucket    # real AWS: the default profile, or add --profile
python s3_cache_demo.py -m -d my_data_dir     # the directories are made in my_data_dir
```

Every option has a one letter form: `-b` (`--bucket`), `-p` (`--profile`), `-m` (`--mock`), `-c` (`--count`), `-s` (`--size`),
`-k` (`--keep`) and `-d` (`--data-dir`). `python s3_cache_demo.py --help` lists them.

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

### 2. Set up the two AWS profiles that the examples use

The examples use two AWS profiles, to show both ways of giving `awsimple` your credentials:

| Profile | Used by | Needs access to |
|---|---|---|
| your default profile | `aws_access_test.py`, `write_read_s3_object.py`, `read_s3_object.py` (and `s3_cache_demo.py --bucket`, unless you give it `--profile`) | S3 |
| a profile named `testawsimple` | `derived_access_class.py`, `dynamodb_partition_only.py`, `dynamodb_partition_and_sort.py` | S3 and DynamoDB |

Both can be for the same AWS account and user. If they are for different accounts, `derived_access_class.py` can not use the bucket
that `write_read_s3_object.py` made, so change the bucket name in one of them. `s3_cache_demo.py --mock` and the `pubsub` example
(your default profile, with access to SNS and SQS) are the exceptions: the first needs no AWS account.

```
aws configure
aws configure --profile testawsimple
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
