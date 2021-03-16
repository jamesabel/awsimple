import os
import pytest
from pathlib import Path
import logging


from balsa import Balsa

from awsimple import __application_name__, __author__, is_mock, use_moto_mock_env_var

mock_env_var = os.environ.get(use_moto_mock_env_var)

if mock_env_var is None:
    os.environ[use_moto_mock_env_var] = "1"


class TestAWSimpleLoggingHandler(logging.Handler):
    def emit(self, record):
        print(record.getMessage())
        assert False


@pytest.fixture(scope="session", autouse=True)
def session_fixture():

    balsa = Balsa(__application_name__, __author__, log_directory=Path("log"), delete_existing_log_files=True, verbose=False)

    # add handler that will throw an assert on ERROR or greater
    test_handler = TestAWSimpleLoggingHandler()
    test_handler.setLevel(logging.ERROR)
    logging.getLogger().addHandler(test_handler)

    balsa.init_logger()

    print(f"{is_mock()=}")
