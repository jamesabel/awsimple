import time
from datetime import timedelta
from pathlib import Path

from awsimple import S3Access
from test_awsimple import test_awsimple_str

temp_dir = Path("temp")


def test_s3():

    test_string = str(time.time())  # so it changes between tests

    s3_access = S3Access(profile_name=test_awsimple_str, bucket=test_awsimple_str)
    assert s3_access.create_bucket()
    s3_access.write_string(test_string, test_awsimple_str)
    assert s3_access.read_string(test_awsimple_str) == test_string


def test_aws_big_file():
    # make a big file

    temp_dir.mkdir(parents=True, exist_ok=True)

    big_file_name = "big.txt"
    big_last_run_file_path = Path(temp_dir, "big_last_run.txt")

    try:
        last_run = float(big_last_run_file_path.open().read().strip())
    except FileNotFoundError:
        last_run = 0.0

    # only run once a day max since it takes so long
    if last_run + timedelta(days=7).total_seconds() < time.time():

        big_file_path = Path(temp_dir, big_file_name)
        max_size = round(100E6)  # should be large enough to do a multi-part upload and would timeout with default AWS timeouts (we use longer timeouts than the defaults)
        size = max_size/1000  # start with something small
        while size < max_size:
            size *= 2  # get bigger on each iteration
            size = min(max_size, size)  # make sure at the end we do one of max size
            with big_file_path.open("w") as f:
                f.truncate(round(size))  # this quickly makes a (sparse) file filled with zeros
            start = time.time()
            s3_access = S3Access(profile_name=test_awsimple_str, bucket=test_awsimple_str)
            s3_access.upload(big_file_path, big_file_name)
            print(f"{time.time() - start},{size:.0f}")

        big_last_run_file_path.open('w').write(str(time.time()))
    else:
        print(f"last run {time.time() - last_run} seconds ago so not running now")
