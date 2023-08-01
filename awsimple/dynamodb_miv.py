from typing import Union, Type
import time
from copy import deepcopy
from logging import getLogger
from decimal import Decimal

from typeguard import typechecked
from boto3.dynamodb.conditions import Key

from awsimple import DynamoDBAccess, DBItemNotFound, __application_name__

miv_string = "mivui"  # monotonically increasing value roughly based on uS (microseconds) since the epoch, as an integer

log = getLogger(__application_name__)


@typechecked()
def get_time_us() -> int:
    """
    Get the current time in uS (microseconds) since the epoch as an int.
    :return: time in uS since the epoch
    """
    return int(round(time.time() * 1e6))


@typechecked()
def miv_us_to_timestamp(miv_ui: Union[int, Decimal]) -> float:
    """
    Convert a miv uS int back to regular timestamp since epoch in seconds.
    :param miv_ui: MIV in uS as an int
    :return: regular time since epoch in seconds (as a float)
    """
    return float(miv_ui) / 1e6


class DynamoDBMIVUI(DynamoDBAccess):
    """
    DynamoDB with a MIV UI (monotonically increasing value in uS since the epoch as an integer) as the "sort" key of the primary key pair. Useful for ordered puts and gets to DynamoDB,
    and enables get-ing the most senior item.

    One of the complaints about DynamoDB is that it doesn't have "automatic indexing" and/or "automatic timestamp". While this isn't automatic indexing per se, it does provide for
    ordered writes for a given primary partition (hash) key, and does so via a monotonically increasing value roughly based on time (essentially an automatic timestamp), which in
    some cases may be even more useful.
    """

    @typechecked()
    def create_table(  # type: ignore
        self,
        partition_key: str,
        secondary_index: Union[str, None] = None,
        partition_key_type: Union[Type[str], Type[int], Type[bool]] = str,
        secondary_key_type: Union[Type[str], Type[int], Type[bool]] = str,
    ) -> bool:
        return super().create_table(partition_key, miv_string, secondary_index, partition_key_type, int, secondary_key_type)

    @typechecked()
    def put_item(self, item: dict, time_us: Union[int, None] = None):
        """
        Put (write) a DynamoDB table item with the miv automatically filled in.

        :param item: item
        :param time_us: optional time in uS to use (otherwise current time is used)
        """
        assert self.resource is not None
        table = self.resource.Table(self.table_name)

        # Determine new miv. The miv is an int to avoid comparison or specification problems that can arise with floats. For example, when it comes time to delete an item.
        if time_us is None:
            # get the miv for the existing entries
            partition_key = self.get_primary_partition_key()
            partition_value = item[partition_key]
            try:
                existing_most_senior_item = self.get_most_senior_item(partition_key, partition_value)
                existing_miv_ui = existing_most_senior_item[miv_string]
            except DBItemNotFound:
                existing_miv_ui = None

            current_time_us = get_time_us()
            if existing_miv_ui is None or current_time_us > existing_miv_ui:
                new_miv_ui = current_time_us
            else:
                # the prior writer seems to be from the future (from our perspective), so just increment the existing miv by the smallest increment and go with that
                new_miv_ui = existing_miv_ui + 1
        else:
            new_miv_ui = time_us

        # make the new item with the new miv and put it into the DB table
        new_item = deepcopy(item)
        new_item[miv_string] = new_miv_ui
        table.put_item(Item=new_item)

    @typechecked()
    def get_most_senior_item(self, partition_key: str, partition_value: Union[str, int]) -> dict:
        """
        Get the most senior (greatest miv value) item for a given primary partition (hash) key. Raises DBItemNotFound if it doesn't exist.
        :return: most senior item
        """
        assert self.resource is not None
        table = self.resource.Table(self.table_name)
        # just get the one most senior item
        response = table.query(KeyConditionExpression=Key(partition_key).eq(partition_value), ScanIndexForward=False, Limit=1)
        if (items := response.get("Items")) is None or len(items) < 1:
            raise DBItemNotFound(f"{partition_key=},{partition_value=}")
        item = items[0]  # we asked for exactly one
        return item
