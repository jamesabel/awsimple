# Running the examples for awsimple

There are four examples in the example folder. When run, these examples should
 - check that you have access to aws through the aws cli
 - write a file called "hello.txt" to the S3 bucket awsimple-test-bucket-{random_number}
     - Note: It is strongly recommended to change the bucket name before you run this, but it will work without it
 - read the file from the S3 bucket awsimple-test-bucket-{random_number}



### 1. Make the Virtual Environment and activate it

#### Mac / Linux
```
source make_venv.sh
./venv/bin/activate
```

#### Windows
```
make_venv.bat
.\venv\Script\activate.bat
```

### 2. Check your AWS profile and create a test user name "testawsimple" with read/write access to s3.

Your default aws profile should be setup before you run the examples. The examples use a test user named "testawsimple". You should create this user before running the examples.

```
aws config
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
