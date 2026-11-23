"""
The CRC64NVME of bytes and of files, which is S3's native full-object checksum of the objects awsimple writes.

Every CRC64NVME that awsimple calculates is calculated here, with get_bytes_crc64nvme() and get_file_crc64nvme(), so this is the one place where the implementation
is chosen. The implementations give the same value (16 lower case hex digits) and differ in speed:

- "awscrt": the AWS Common Runtime (C, with the CPU's CRC instructions), which boto3 itself uses for the CRC64NVME of an upload. Thousands of MiB/s.
- "hashy": pure Python. Tens of MiB/s. Used if awscrt can not be imported (e.g. a Python version or platform it has no wheel for).

To use another implementation, make a Crc64NvmeImplementation for it and add it to _get_implementations(), before the ones it is preferred to.
"""

from dataclasses import dataclass
from functools import cache
from logging import getLogger
from pathlib import Path
from typing import Callable, List, Union

import hashy

from .__version__ import __application_name__

log = getLogger(__application_name__)

file_chunk_size = 8 * 1024 * 1024  # a file is read this many bytes at a time


@dataclass(frozen=True)
class Crc64NvmeImplementation:
    """
    One way to calculate a CRC64NVME.
    """

    name: str
    get_bytes: Callable[[bytes], str]  # CRC64NVME of bytes, as 16 lower case hex digits
    get_file: Callable[[Path], str]  # CRC64NVME of the contents of a file, as 16 lower case hex digits


def _to_hex(crc64nvme: int) -> str:
    """
    :param crc64nvme: CRC64NVME as an integer
    :return: CRC64NVME as 16 lower case hex digits (what S3 gives, once decoded from base64)
    """
    return format(crc64nvme, "016x")


def _get_awscrt_implementation() -> Union[Crc64NvmeImplementation, None]:
    """
    :return: the implementation with the AWS Common Runtime, or None if it can not be imported
    """
    try:
        from awscrt import checksums
    except ImportError as e:
        log.info(f"awscrt is not available for CRC64NVME ({e})")
        return None

    def _get_bytes(data: bytes) -> str:
        return _to_hex(checksums.crc64nvme(data))

    def _get_file(path: Path) -> str:
        crc64nvme = 0
        with open(path, "rb") as file:
            while chunk := file.read(file_chunk_size):
                crc64nvme = checksums.crc64nvme(chunk, crc64nvme)
        return _to_hex(crc64nvme)

    return Crc64NvmeImplementation("awscrt", _get_bytes, _get_file)


def _get_hashy_implementation() -> Crc64NvmeImplementation:
    """
    :return: the pure Python implementation
    """
    return Crc64NvmeImplementation("hashy", hashy.get_bytes_crc64nvme, hashy.get_file_crc64nvme)


def _get_implementations() -> List[Crc64NvmeImplementation]:
    """
    :return: the implementations that can be used here, the preferred (fastest) one first
    """
    implementations = []
    if (awscrt_implementation := _get_awscrt_implementation()) is not None:
        implementations.append(awscrt_implementation)
    implementations.append(_get_hashy_implementation())
    return implementations


@cache
def get_crc64nvme_implementation() -> Crc64NvmeImplementation:
    """
    Get the implementation that awsimple calculates a CRC64NVME with (chosen once per process).

    :return: the implementation
    """
    implementation = _get_implementations()[0]
    log.info(f"CRC64NVME is calculated with {implementation.name}")
    return implementation


def get_bytes_crc64nvme(data: bytes) -> str:
    """
    Get the CRC64NVME of bytes.

    :param data: the bytes
    :return: CRC64NVME as 16 lower case hex digits
    """
    return get_crc64nvme_implementation().get_bytes(data)


def get_file_crc64nvme(path: Path) -> str:
    """
    Get the CRC64NVME of the contents of a file.

    :param path: path to the file
    :return: CRC64NVME as 16 lower case hex digits
    """
    return get_crc64nvme_implementation().get_file(path)
