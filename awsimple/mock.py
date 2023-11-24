import os
from functools import cache

from tobool import to_bool_strict

use_moto_mock_env_var = "AWSIMPLE_USE_MOTO_MOCK"
use_localstack_env_var = "AWSIMPLE_USE_LOCALSTACK"


@cache
def is_mock() -> bool:
    """
    Is using moto mock?
    :return: True if using moto mock.
    """
    return to_bool_strict(os.environ.get(use_moto_mock_env_var, "0"))


@cache
def is_using_localstack() -> bool:
    """
    Is using localstack?
    :return: True if using localstack.
    """
    return to_bool_strict(os.environ.get(use_localstack_env_var, "0"))
