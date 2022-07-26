from typing import Union, Type
import time
from copy import deepcopy
from logging import getLogger

from typeguard import typechecked
from boto3.dynamodb.conditions import Key

from awsimple import DynamoDBAccess, DBItemNotFound, __application_name__

miv_string = "miv"

log = getLogger(__application_name__)


def get_time_us() -> int:
    """
    Get the current time in uS (microseconds) since the epoch.
    :return: time in uS since the epoch
    """
    return int(round(time.time() * 1E6))


class DynamoDBMIV(DynamoDBAccess):
    """
    DynamoDB with a MIV (monotonically increasing value) as the "sort" key of the primary key pair. Useful for ordered puts and gets to DynamoDB, and enables get-ing the
    most senior item.
    """

    @typechecked()
    def create_table(
        self,
        partition_key: str,
        secondary_index: str = None,
        partition_key_type: Union[Type[str], Type[int], Type[bool]] = str,
        secondary_key_type: Union[Type[str], Type[int], Type[bool]] = str,
    ) -> bool:
        return super().create_table(partition_key, miv_string, secondary_index, partition_key_type, int, secondary_key_type)

    @typechecked()
    def put_item(self, item: dict):
        """
        Put (write) a DynamoDB table item. The miv is automatically filled in.

        :param item: item
        """
        table = self.resource.Table(self.table_name)

        # get the miv for the existing entries
        partition_key, sort_key = self.get_primary_keys()
        try:
            existing_most_senior_item = self.get_most_senior_item(partition_key, item[partition_key])
            existing_miv = existing_most_senior_item[miv_string]
        except DBItemNotFound:
            existing_miv = None

        # determine new miv
        time_us = get_time_us()
        if existing_miv is None or time_us > existing_miv:
            new_miv = time_us
        else:
            new_miv = existing_miv + 1  # the prior writer seems to be from the future, so increment the existing miv by the smallest increment and go with that

        # make the new item with the new miv and put it into the DB table
        new_item = deepcopy(item)
        new_item[miv_string] = new_miv
        table.put_item(Item=new_item)

    @typechecked()
    def get_most_senior_item(self, partition_key: str, partition_value: Union[str, int]) -> dict:
        """
        Get the most senior (greatest miv value) item for a given primary partition (hash) key. Raises DBItemNotFound if it doesn't exist.
        :return: most senior item
        """
        table = self.resource.Table(self.table_name)
        # just get the one most senior item
        response = table.query(KeyConditionExpression=Key(partition_key).eq(partition_value), ScanIndexForward=False, Limit=1)
        if (item := response.get("Item")) is None:
            raise DBItemNotFound(f"{partition_key=},{partition_value=}")
        return item
