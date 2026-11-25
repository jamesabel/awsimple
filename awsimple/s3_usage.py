"""
What an S3Access has used of S3 that AWS charges for: the requests it made and the bytes S3 sent it.

AWS charges for S3 requests by request tier and by the storage class of the object, and for the bytes of the objects that are read (data transfer out of AWS, and
a retrieval fee for some storage classes). This module counts them. It has no prices: AWS's prices differ by region and change, so pricing is up to the caller.

The counts are of the S3 API calls of the boto3 clients of an S3Access, whoever makes them: awsimple, the transfer manager of a download, or the caller using
S3Access.client directly. They are taken from botocore's "after-call" event, so:

- A call that botocore retried is counted once.
- A call that S3 answered with an error is counted (AWS charges for a request that is refused, e.g. a 403 or a 404). The storage class of such a call is not known,
  so it is counted as STANDARD.
- A call that never got an answer (e.g. no connection) is not counted.
"""

from collections import defaultdict
from enum import Enum
from threading import Lock
from typing import Any, Dict, Tuple, Union

from typeguard import typechecked

# S3 only says what the storage class of an object is when it is not this one
standard_storage_class = "STANDARD"

# The start of the names of the S3 API operations that are not charged for, and of those charged as tier 1. Every other operation is charged as tier 2.
free_operation_prefixes = ("Delete", "Abort")
tier1_operation_prefixes = ("Put", "Copy", "Post", "List", "Create", "Upload", "Complete", "Restore")


class S3RequestTier(Enum):
    """
    The request tiers AWS prices S3 requests by (the "Requests-Tier1" and "Requests-Tier2" usage types of an AWS bill).
    """

    tier1 = "tier1"  # PUT, COPY, POST and LIST requests
    tier2 = "tier2"  # GET, HEAD and all other requests
    free = "free"  # DELETE and CANCEL requests, which are not charged for


@typechecked()
def get_s3_request_tier(operation: str) -> S3RequestTier:
    """
    The request tier AWS charges an S3 API operation as. This goes by the operation's name, which is right for the operations on objects and bucket listings (what
    awsimple does). AWS's price list is what is definitive.

    :param operation: S3 API operation name as botocore has it, e.g. "HeadObject" or "ListObjectsV2"
    :return: the request tier
    """
    if operation.startswith(free_operation_prefixes):
        tier = S3RequestTier.free
    elif operation.startswith(tier1_operation_prefixes):
        tier = S3RequestTier.tier1
    else:
        tier = S3RequestTier.tier2
    return tier


class S3Usage:
    """
    The S3 requests that were made and the bytes S3 sent, by storage class (see this module's docstring for what is counted). An S3Access has one, as its .usage.
    It is safe to use from several threads: a sync and a verification make requests from many.
    """

    def __init__(self):
        self._lock = Lock()
        self._request_counts: Dict[Tuple[str, str], int] = defaultdict(int)
        self._download_sizes: Dict[str, int] = defaultdict(int)

    def instrument(self, client: Any):
        """
        Count the S3 API calls of a boto3 S3 client from now on.

        :param client: boto3 S3 client
        """
        client.meta.events.register("after-call.s3", self._after_call)

    def _after_call(self, parsed: Dict[str, Any], model: Any, **kwargs):
        """
        botocore's "after-call" event: S3 has answered an API call.

        :param parsed: the parsed response
        :param model: botocore's model of the operation
        :param kwargs: what else the event has
        """
        operation = model.name
        # only the bytes of an object that S3 sends are counted (for a GetObject of a range, that is the length of the range)
        is_object_read = operation == "GetObject" and "Error" not in parsed
        self.record(operation, parsed.get("StorageClass", standard_storage_class), parsed.get("ContentLength", 0) if is_object_read else 0)

    @typechecked()
    def record(self, operation: str, storage_class: str = standard_storage_class, download_size: int = 0):
        """
        Count one S3 request.

        :param operation: S3 API operation name as botocore has it, e.g. "HeadObject"
        :param storage_class: storage class of the object the request was for (STANDARD for a request that is not for one object, e.g. a bucket listing)
        :param download_size: the bytes of the object that S3 sent in answer
        """
        with self._lock:
            self._request_counts[(operation, storage_class)] += 1
            if download_size > 0:
                self._download_sizes[storage_class] += download_size

    @typechecked()
    def get_request_counts(self) -> Dict[Tuple[str, str], int]:
        """
        :return: how many requests were made, by S3 API operation name and storage class, e.g. {("HeadObject", "GLACIER_IR"): 3}
        """
        with self._lock:
            return dict(self._request_counts)

    @typechecked()
    def get_request_count(self, operation: Union[str, None] = None, storage_class: Union[str, None] = None) -> int:
        """
        :param operation: only count the requests of this S3 API operation (None for every operation)
        :param storage_class: only count the requests for objects of this storage class (None for every storage class)
        :return: how many requests were made
        """
        return sum(
            count
            for (counted_operation, counted_storage_class), count in self.get_request_counts().items()
            if operation in (None, counted_operation) and storage_class in (None, counted_storage_class)
        )

    @typechecked()
    def get_tier_counts(self) -> Dict[Tuple[S3RequestTier, str], int]:
        """
        The requests as AWS prices them (see get_s3_request_tier()).

        :return: how many requests were made, by request tier and storage class, e.g. {(S3RequestTier.tier2, "GLACIER_IR"): 3}
        """
        tier_counts: Dict[Tuple[S3RequestTier, str], int] = defaultdict(int)
        for (operation, storage_class), count in self.get_request_counts().items():
            tier_counts[(get_s3_request_tier(operation), storage_class)] += count
        return dict(tier_counts)

    @typechecked()
    def get_download_sizes(self) -> Dict[str, int]:
        """
        The bytes of S3 objects that S3 sent, which is what data transfer out and the retrieval fee of a storage class are charged for. A download that failed part
        way counts in full, and so does each attempt of one that was tried again.

        :return: the bytes S3 sent, by storage class
        """
        with self._lock:
            return dict(self._download_sizes)

    @typechecked()
    def get_download_size(self) -> int:
        """
        :return: the bytes of S3 objects that S3 sent, of every storage class
        """
        return sum(self.get_download_sizes().values())

    def reset(self):
        """
        Start counting from zero again.
        """
        with self._lock:
            self._request_counts.clear()
            self._download_sizes.clear()
