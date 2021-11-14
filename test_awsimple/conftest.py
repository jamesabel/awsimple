import os
import pytest
from pathlib import Path
import logging


from balsa import Balsa

from awsimple import __application_name__, __author__, is_mock, use_moto_mock_env_var, S3Access

from test_awsimple import test_awsimple_str, temp_dir, cache_dir

mock_env_var = os.environ.get(use_moto_mock_env_var)

if mock_env_var is None:
    os.environ[use_moto_mock_env_var] = "1"

# if using non-local pytest, create the credentials and config files dynamically
aws_credentials_and_config_dir = Path(Path.home(), ".aws")
aws_credentials_file = Path(aws_credentials_and_config_dir, "credentials")
aws_config_file = Path(aws_credentials_and_config_dir, "config")
if is_mock():
    if not aws_credentials_and_config_dir.exists():
        aws_credentials_and_config_dir.mkdir(parents=True, exist_ok=True)
    if not aws_credentials_file.exists():
        credential_strings = [
            "[default]\naws_access_key_id=AAAAAAAAAAAAAAAAAAAA\naws_secret_access_key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            f"[{test_awsimple_str}]\naws_access_key_id=AAAAAAAAAAAAAAAAAAAA\naws_secret_access_key=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        ]
        aws_credentials_file.write_text("\n".join(credential_strings))
    if not aws_config_file.exists():
        config_strings = ["[profile default]\nregion=us-west-2", f"[profile {test_awsimple_str}]\nregion=us-west-2"]
        aws_config_file.write_text("\n".join(config_strings))


class TestAWSimpleLoggingHandler(logging.Handler):
    def emit(self, record):
        print(record.getMessage())
        assert False


@pytest.fixture(scope="session", autouse=True)
def session_fixture():

    temp_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    balsa = Balsa(__application_name__, __author__, log_directory=Path("log"), delete_existing_log_files=True, verbose=False)

    # add handler that will throw an assert on ERROR or greater
    test_handler = TestAWSimpleLoggingHandler()
    test_handler.setLevel(logging.ERROR)
    logging.getLogger().addHandler(test_handler)

    balsa.init_logger()

    print(f"{is_mock()=}")


@pytest.fixture(scope="module")
def s3_access():
    _s3_access = S3Access(profile_name=test_awsimple_str, bucket_name=test_awsimple_str, cache_dir=cache_dir)
    return _s3_access
