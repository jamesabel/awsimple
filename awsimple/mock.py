"""
Which AWS awsimple accesses: the real one (the default) or the moto mock, selected with an environment variable.
"""

import os

from tobool import to_bool_strict

use_moto_mock_env_var = "AWSIMPLE_USE_MOTO_MOCK"


def is_mock() -> bool:
    """
    Is using moto mock?
    :return: True if using moto mock.
    """
    return to_bool_strict(os.environ.get(use_moto_mock_env_var, "0"))
