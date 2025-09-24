from functools import cache

import getpass
import platform


@cache
def get_user_name() -> str:
    return getpass.getuser()


@cache
def get_computer_name() -> str:
    return platform.node()


@cache
def get_node_name() -> str:
    node_name = f"{get_computer_name()}-{get_user_name()}"  # AWS SNS and SQS names can include hyphens
    return node_name
