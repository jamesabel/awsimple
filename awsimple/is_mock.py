import os
from distutils.util import strtobool


use_moto_mock_env_var = "AWSIMPLE_USE_MOTO_MOCK"


def is_mock() -> bool:
    return bool(strtobool(os.environ.get(use_moto_mock_env_var, '0')))
