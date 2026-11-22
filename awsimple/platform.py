"""
Names of the computer and user this process runs as, for AWS resource names that are per node (e.g. a log stream or a pub/sub queue).
"""

from functools import cache

import getpass
import platform


@cache
def get_user_name() -> str:
    """
    :return: the name of the user this process runs as
    """
    return getpass.getuser()


@cache
def get_computer_name() -> str:
    """
    :return: the network name of this computer
    """
    return platform.node()


@cache
def get_node_name() -> str:
    """
    :return: a name for this computer and user, safe for AWS SNS and SQS names
    """
    node_name = f"{get_computer_name()}-{get_user_name()}"  # AWS SNS and SQS names can include hyphens
    return node_name
