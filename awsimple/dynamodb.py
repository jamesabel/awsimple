"""
DynamoDB access
"""

import io
import decimal
import pickle
import time
from collections import OrderedDict, defaultdict, namedtuple
import datetime
from pathlib import Path
from os.path import getsize, getmtime
from typing import List, Union, Any, Type, Dict, Callable
from pprint import pformat
from itertools import islice
import json
from enum import Enum
from decimal import Decimal
from logging import getLogger

from boto3.exceptions import RetriesExceededError
from botocore.exceptions import EndpointConnectionError, ClientError
from boto3.dynamodb.conditions import Key
from typeguard import typechecked
from dictim import dictim

from awsimple import CacheAccess, __application_name__, AWSimpleException

# don't require pillow, but convert images with it if it exists
pil_exists = False
try:
    from PIL import Image

    pil_exists = True
except ImportError:
    pass

# Handles Inexact error.
decimal_context = decimal.getcontext().copy()
decimal_context.prec = 38  # Numbers can have 38 digits precision
handle_inexact_error = True

# for scan to dict
DictKey = namedtuple("DictKey", ["partition", "sort"])  # only for Primary Key with both partition and sort keys

log = getLogger(__application_name__)


class QuerySelection(Enum):
    lowest = 0
    highest = 1


def convert_serializable_special_cases(o):
    """
    Convert an object to a type that is fairly generally serializable (e.g. json serializable).
    This only handles the cases that need converting.  The json module handles all the rest.
    For JSON, with json.dump or json.dumps with argument default=convert_serializable.
    Example:
    json.dumps(my_animal, indent=4, default=convert_serializable)

    :param o: object to be converted to a type that is serializable
    :return: a serializable representation
    """

    if isinstance(o, Enum):
        serializable_representation = o.name
    elif isinstance(o, Decimal):
        # decimal.Decimal (e.g. in AWS DynamoDB), both integer and floating point

        try:
            is_int = o % 1 == 0  # doesn't work for numbers greater than decimal.MAX_EMAX
        except decimal.InvalidOperation:
            is_int = False  # numbers larger than decimal.MAX_EMAX will get a decimal.DivisionImpossible, so we'll just have to represent those as a float

        if is_int:
            # if representable with an integer, use an integer
            serializable_representation = int(o)
        else:
            # not representable with an integer so use a float
            serializable_representation = float(o)
    elif isinstance(o, bytes) or isinstance(o, bytearray):
        serializable_representation = str(o)
    elif hasattr(o, "value"):
        # e.g. PIL images
        serializable_representation = str(o.value)
    else:
        raise NotImplementedError(f"can not serialize {o} since type={type(o)}")
    return serializable_representation


@typechecked()
def dynamodb_to_json(item, indent=None) -> str:
    """
    Convert a DynamoDB item to JSON

    :param item: DynamoDB item
    :param indent: JSON indent
    :return: JSON encoded string
    """
    return json.dumps(item, default=convert_serializable_special_cases, sort_keys=True, indent=indent)


@typechecked()
def dynamodb_to_dict(item) -> dict:
    """

    Convert a DynamoDB item to a serializable dict

    :param item: DynamoDB item
    :return: serializable dict
    """

    return json.loads(dynamodb_to_json(item))


@typechecked()
def dict_to_dynamodb(input_value: Any, convert_images: bool = True, raise_exception: bool = True) -> Any:
    """
    makes a dictionary follow boto3 item standards

    :param input_value: input dictionary
    :param convert_images: set to False to skip over images (they can be too large)
    :param raise_exception: set to False to not raise exceptions on issues

    :return: converted version of the original dictionary

    """
    resp = None  # type: Any
    if type(input_value) is dict or type(input_value) is OrderedDict or type(input_value) is defaultdict or type(input_value) is dictim:
        if type(input_value) is dictim:
            resp = dict(input_value)
        else:
            resp = {}
        for k, v in input_value.items():
            if type(k) is int:
                k = str(k)  # allow int as key since it is unambiguous (e.g. bool and float are ambiguous)
            resp[k] = dict_to_dynamodb(v, convert_images, raise_exception)
    elif type(input_value) is list or type(input_value) is tuple:
        # converts tuple to list
        resp = [dict_to_dynamodb(v, convert_images, raise_exception) for v in input_value]
    elif type(input_value) is str:
        # DynamoDB does not allow zero length strings
        if len(input_value) > 0:
            resp = input_value
    elif type(input_value) is bool or input_value is None or type(input_value) is decimal.Decimal:
        resp = input_value  # native DynamoDB types
    elif type(input_value) is float or type(input_value) is int:
        # boto3 uses Decimal for numbers
        # Handle the 'inexact error' via decimal_context.create_decimal
        # 'casting' to str may work as well, but decimal_context.create_decimal should be better at maintaining precision
        if handle_inexact_error:
            resp = decimal_context.create_decimal(input_value)
        else:
            resp = decimal.Decimal(input_value)
    elif isinstance(input_value, Enum):
        resp = input_value.name
    elif isinstance(input_value, bytes):
        resp = str(input_value)
    elif convert_images and pil_exists and isinstance(input_value, Image.Image):
        # save pillow (PIL) image as PNG binary
        image_byte_array = io.BytesIO()
        input_value.save(image_byte_array, format="PNG")
        resp = image_byte_array.getvalue()
    elif isinstance(input_value, datetime.datetime):
        resp = input_value.isoformat()
    else:
        if raise_exception:
            raise NotImplementedError(type(input_value), input_value)
    return resp


class DBItemNotFound(AWSimpleException):
    def __init__(self, key):
        self.key = key
        self.message = "Item not found"
        super().__init__(self.message)

    def __str__(self):
        return f"{self.key=} {self.message}"


@typechecked()
def _is_valid_db_pickled_file(file_path: Path, cache_life: Union[float, int, None]) -> bool:
    is_valid = file_path.exists() and getsize(str(file_path)) > 0
    if is_valid and cache_life is not None:
        is_valid = time.time() <= getmtime(str(file_path)) + cache_life
    return is_valid


class DynamoDBAccess(CacheAccess):
    @typechecked()
    def __init__(self, table_name: str = None, **kwargs):
        """
        AWS DynamoDB access

        :param table_name: DynamoDB table name
        :param kwargs: kwargs
        """
        self.table_name = table_name  # can be None (the default) if we're only doing things that don't require a table name such as get_table_names()
        self.cache_hit = False
        self.secondary_index_postfix = "-index"
        super().__init__(resource_name="dynamodb", **kwargs)

    @typechecked()
    def get_table_names(self) -> List[str]:
        """
        get all DynamoDB tables

        :return: a list of DynamoDB table names
        """

        table_names = []
        more_to_evaluate = True
        last_evaluated_table_name = None
        while more_to_evaluate:
            if last_evaluated_table_name is None:
                response = self.client.list_tables()
            else:
                response = self.client.list_tables(ExclusiveStartTableName=last_evaluated_table_name)
            partial_table_names = response.get("TableNames")
            last_evaluated_table_name = response.get("LastEvaluatedTableName")
            if partial_table_names is not None and len(partial_table_names) > 0:
                table_names.extend(partial_table_names)
            if last_evaluated_table_name is None:
                more_to_evaluate = False
        table_names.sort()

        return table_names

    def rows_to_dict(self, rows: list, sort_key: Union[Callable, None] = None) -> dict:
        """
        Get table rows as a sorted dict. dict key is the DynamoDB primary key, either as a single value (if only Partition Key used) or a 2 element named tuple (if Partition and Sort key used).
        Input row ends up being sorted.

        :param rows: table rows (sorted after call)
        :param sort_key: function to use to get the sort key from the row or omit to sort based on DynamoDB Primary Key
        :return: dict of data with Primary Key used as key
        """

        db_partition_key, db_sort_key = self.get_primary_keys()  # DynamoDB Primary Keys

        if sort_key is None:
            # if a sort key for the output isn't provided by the caller, use the DynamoDB Primary Key
            if db_sort_key is None:
                rows.sort(key=lambda x: x[db_partition_key])
            else:
                rows.sort(key=lambda x: (x[db_partition_key], x[db_sort_key]))
        else:
            rows.sort(key=sort_key)  # caller provided sort

        table_as_dict = {}
        if db_sort_key is None:
            for row in rows:
                table_as_dict[row[db_partition_key]] = row
        else:
            for row in rows:
                table_as_dict[DictKey(row[db_partition_key], row[db_sort_key])] = row

        return table_as_dict

    @typechecked()
    def scan_table(self) -> list:
        """
        returns entire lookup table

        :param table_name: DynamoDB table name
        :param profile_name: AWS IAM profile name
        :return: table contents
        """

        # todo: use boto3 paginator

        items = []
        table = self.resource.Table(self.table_name)

        more_to_evaluate = True
        exclusive_start_key = None
        while more_to_evaluate:
            try:
                if exclusive_start_key is None:
                    response = table.scan()
                else:
                    response = table.scan(ExclusiveStartKey=exclusive_start_key)
            except EndpointConnectionError as e:
                log.warning(e)
                response = None
                more_to_evaluate = False

            if response is not None:
                items.extend(response["Items"])
                if "LastEvaluatedKey" not in response:
                    more_to_evaluate = False
                else:
                    exclusive_start_key = response["LastEvaluatedKey"]

        log.info(f"read {len(items)} items from {self.table_name}")

        return items

    @typechecked()
    def scan_table_as_dict(self, sort_key: Union[Callable, None] = None) -> dict:
        """
        Scan table and return result as a dict with the key being the table's Primary Key.

        :param sort_key:
        :param sort_key: function to use to get the sort key from the row or omit to sort based on DynamoDB Primary Key
        :return: dict of data with Primary Key used as key
        """
        return self.rows_to_dict(self.scan_table(), sort_key)

    @typechecked()
    def scan_table_cached(self, invalidate_cache: bool = False) -> list:
        """

        Read data table(s) from AWS with caching.  This is meant for static or slowly changing tables.

        :param invalidate_cache: True to initially invalidate the cache (forcing a table scan)
        :return: a list with the (possibly cached) table data
        """

        self.cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file_path = Path(self.cache_dir, f"{self.table_name}.pickle")
        log.debug(f"cache_file_path : {cache_file_path.resolve()}")

        if invalidate_cache:
            cache_file_path.unlink(missing_ok=True)  # invalidate by deleting the cache file

        table_data_valid = False
        table_data = []
        if _is_valid_db_pickled_file(cache_file_path, self.cache_life):
            with open(cache_file_path, "rb") as f:
                log.info(f"{self.table_name=} : reading {cache_file_path=}")
                table_data = pickle.load(f)
                table_data_valid = True
                log.debug(f"done reading {cache_file_path=}")

                # If the DynamoDB table has more entries than what's in our cache, then we deem our cache to be stale.  The table count updates approximately
                # every 6 hours.  The assumption here is that we're generally adding items to the table, and if the table has more items than we
                # have in our cache, we need to update our cache even if we haven't had a timeout.
                item_count_mismatch = self.resource.Table(self.table_name).item_count != len(table_data)

        if not table_data_valid or item_count_mismatch:
            log.info(f"getting {self.table_name} from DB")

            try:
                table_data = self.scan_table()
                table_data_valid = True
            except RetriesExceededError:
                pass

            if table_data_valid:
                # update data cache
                with open(cache_file_path, "wb") as f:
                    pickle.dump(table_data, f)

            self.cache_hit = False
        else:
            self.cache_hit = True

        if not table_data_valid:
            AWSimpleException(f'table "{self.table_name}" not accessible')

        return table_data

    @typechecked()
    def scan_table_cached_as_dict(self, invalidate_cache: bool = False, sort_key: Union[Callable, None] = None) -> dict:
        """
        Scan table and return result as a dict with the key being the table's Primary Key.

        :param invalidate_cache: True to initially invalidate the cache (forcing a table scan)
        :param sort_key: function to use to get the sort key from the row or omit to sort based on DynamoDB Primary Key
        :return: dict of data with Primary Key used as key
        """
        return self.rows_to_dict(self.scan_table_cached(invalidate_cache), sort_key)

    @typechecked()
    def create_table(
        self,
        partition_key: str,
        sort_key: str = None,
        secondary_index: str = None,
        partition_key_type: Union[Type[str], Type[int], Type[bool]] = str,
        sort_key_type: Union[Type[str], Type[int], Type[bool]] = str,
        secondary_key_type: Union[Type[str], Type[int], Type[bool]] = str,
    ) -> bool:
        """
        Create a DynamoDB table.

        :param partition_key: DynamoDB partition key (AKA hash key)
        :param sort_key: DynamoDB sort key
        :param secondary_index: secondary index key
        :param partition_key_type: partition key type of str, int, bool (str default)
        :param sort_key_type: sort key type of str, int, bool (str default)
        :param secondary_key_type: secondary key type of str, int, bool (str default)
        :return: True if table created
        """

        def add_key(k, t, kt):
            assert t in ("S", "N", "B")  # DynamoDB key types (string, number, bool)
            assert kt in ("HASH", "RANGE")
            definition = {"AttributeName": k, "AttributeType": t}
            schema = {"AttributeName": k, "KeyType": kt}
            return definition, schema

        def type_to_attribute_type(t):
            if t is str:
                ts = "S"
            elif t is int:
                ts = "N"
            elif t is bool:
                ts = "B"
            else:
                raise ValueError(t)
            return ts

        created = False
        if not self.table_exists():
            client = self.client

            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html#HowItWorks.CoreComponents.PrimaryKey

            partition_definition, partition_schema = add_key(partition_key, type_to_attribute_type(partition_key_type), "HASH")  # required
            attribute_definitions = [partition_definition]
            key_schema = [partition_schema]
            if sort_key is not None:
                sort_definition, sort_schema = add_key(sort_key, type_to_attribute_type(sort_key_type), "RANGE")  # optional
                attribute_definitions.append(sort_definition)
                key_schema.append(sort_schema)
            log.info(pformat(key_schema, indent=4))

            kwargs = {
                "AttributeDefinitions": attribute_definitions,
                "KeySchema": key_schema,
                "BillingMode": "PAY_PER_REQUEST",  # on-demand
                "TableName": self.table_name,
            }  # type: Dict[str, Any]

            # add a secondary index, if requested
            if secondary_index is not None:
                index_name = f"{secondary_index}{self.secondary_index_postfix}"
                # currently we only support a single index key (thus the HASH type)
                secondary_definition, secondary_schema = add_key(secondary_index, type_to_attribute_type(secondary_key_type), "HASH")
                # global secondary index does not require the secondary index to be of the same form as the primary
                kwargs["GlobalSecondaryIndexes"] = [{"IndexName": index_name, "KeySchema": [secondary_schema], "Projection": {"ProjectionType": "ALL"}}]
                kwargs["AttributeDefinitions"].append(secondary_definition)

            try:
                client.create_table(**kwargs)
                client.get_waiter("table_exists").wait(TableName=self.table_name)
                created = True
            except ClientError as e:
                log.warning(e)

        return created

    def get_primary_keys(self) -> tuple:
        """
        get the table's primary keys

        :return: a 2-tuple of (partition_key, sort_key).  sort_key will be None if there is no sort key.
        """
        keys = []
        for key_schema in self.resource.Table(self.table_name).key_schema:
            for key_type in ["HASH", "RANGE"]:
                if key_schema["KeyType"] == key_type:
                    keys.append(key_schema["AttributeName"])
        if len(keys) == 0:
            # we should always have a partition key
            raise ValueError(f"no partition key in {self.table_name}")  # should be impossible if DynamoDB is working properly
        elif len(keys) == 1:
            keys.append(None)  # no sort key
        return keys[0], keys[1]

    def _query(self, comp: str, *args) -> List[dict]:
        """
        Query the table with key, value pairs. The first parameter pairs should be the primary key's key/value pairs.  Also supports secondary indexes.

        Examples:
            query(partition_key, partition_value)
            query(partition_key, partition_value, sort_key, sort_value)

        :param comp: compare string (e.g. "eq", "begins_with", etc.)
        :return: a (possibly empty) list of rows matching the query
        """

        primary_keys = self.get_primary_keys()
        secondary_index_name = None

        key_condition_expression = None
        for key, value in zip(islice(args, 0, None, 2), islice(args, 1, None, 2)):
            if key_condition_expression is None:
                key_condition_expression = Key(args[0]).eq(args[1])  # partition key always uses equals (not other queries like "begins_with")
            else:
                key_condition_expression &= getattr(Key(key), comp)(value)
            if key not in primary_keys:
                secondary_index_name = f"{key}{self.secondary_index_postfix}"

        kwargs = {"KeyConditionExpression": key_condition_expression}  # type: Dict[str, Any]
        if secondary_index_name is not None:
            kwargs["IndexName"] = secondary_index_name

        table = self.resource.Table(self.table_name)

        results = []
        more_to_go = True
        last_evaluated_key = None  # pagination
        while more_to_go:
            if last_evaluated_key is not None:
                kwargs["ExclusiveStartKey"] = last_evaluated_key
            query_response = table.query(**kwargs)
            items = query_response.get("Items")
            if items is not None:
                results.extend(items)
            last_evaluated_key = query_response.get("LastEvaluatedKey")
            if last_evaluated_key is None:
                more_to_go = False

        return results

    def query(self, *args) -> List[dict]:
        """
        query exact match

        :param args: key, value pairs
        :return: a list of DB rows matching the query
        """
        return self._query("eq", *args)

    def query_begins_with(self, *args) -> List[dict]:
        """
        query if begins with

        :param args: key, value pairs
        :return: a list of DB rows matching the query
        """
        return self._query("begins_with", *args)

    @typechecked()
    def query_one(self, partition_key: str, partition_value, direction: QuerySelection, secondary_index_name: str = None) -> Union[dict, None]:
        """
        Query and return one or none items, optionally using the sort key to provide either the start or end of the ordered (sorted) set of items.

        This is particularly useful when the table uses a sort key that orders the items and you want one value that is at one of the
        ends of that sort. For example, if the sort key is an epoch timestamp (number) and direction is QueryDirection.highest, the most recent item is returned.

        :param partition_key: partition key
        :param partition_value: partition value to match (exactly)
        :param direction: the range extreme to retrieve (Range.lowest or Range.highest)
        :param secondary_index_name: secondary index (if not using primary index)
        :return: an item or None if not found
        """
        table = self.resource.Table(self.table_name)
        element = None
        scan_index_forward = direction == QuerySelection.lowest  # scanning "backwards" and returning one entry gives us the entry with the greatest sort value
        key_condition_expression = Key(partition_key).eq(partition_value)
        if secondary_index_name is None:
            resp = table.query(KeyConditionExpression=key_condition_expression, ScanIndexForward=scan_index_forward, Limit=1)  # we're just getting one of the ends
        else:
            resp = table.query(IndexName=secondary_index_name, KeyConditionExpression=key_condition_expression, ScanIndexForward=scan_index_forward, Limit=1)
        if resp is not None:
            if (count := resp["Count"]) == 1:
                items = resp["Items"]
                if len(items) == 1:
                    element = resp["Items"][0]
                else:
                    log.error(f"{partition_key=} {partition_value=} {self.table_name=} {len(items)=} (1 expected)")
            elif count > 1:
                log.error(f"{partition_key=} {partition_value=} {self.table_name=} query returned {count=}")
            else:
                log.warning(f'{partition_key=} {partition_value=} not found in table "{self.table_name}"')
        return element

    @typechecked()
    def delete_table(self) -> bool:
        """
        Deletes the current table.
        (This is more like the SQL "DROP TABLE", *not* like a SQL "DELETE", which only deletes rows.  See DynamoDBAccess.delete_all_items() to delete all the items
        but leave the table itself.)

        :return: True if actually deleted, False if it didn't exist in the first place
        """
        timeout_count = 10
        done = False
        deleted_it = False
        while not done and timeout_count > 0:
            try:
                self.client.delete_table(TableName=self.table_name)
                self.client.get_waiter("table_not_exists").wait(TableName=self.table_name)
                deleted_it = True
                done = True
            except self.client.exceptions.ResourceInUseException:
                time.sleep(10)
            except self.client.exceptions.ResourceNotFoundException:
                done = True
            timeout_count -= 1
        return deleted_it

    @typechecked()
    def table_exists(self) -> bool:
        """
        Test if table exists

        :return: True if table exists
        """
        assert self.table_name is not None
        try:
            self.client.describe_table(TableName=self.table_name)
            table_exists = True
        except self.client.exceptions.ResourceNotFoundException:
            table_exists = False
        return table_exists

    @typechecked()
    def put_item(self, item: dict):
        """
        Put (write) a DynamoDB table item

        :param item: item
        """
        table = self.resource.Table(self.table_name)
        table.put_item(Item=item)

    # cant' do a @typechecked() since optional item requires a single type
    def get_item(self, partition_key: str, partition_value: Union[str, int], sort_key: Union[str, None] = None, sort_value: Union[str, int] = None) -> dict:
        """
        get a DB item

        :param partition_key: partition key
        :param partition_value: partition value (str or int)
        :param sort_key: sort key (optional)
        :param sort_value: sort value (optional str or int)
        :return: item dict or raises DBItemNotFound if does not exist
        """
        table = self.resource.Table(self.table_name)
        key = {partition_key: partition_value}  # type: Dict[str, Any]
        if sort_key is not None:
            key[sort_key] = sort_value
        response = table.get_item(Key=key)
        if (item := response.get("Item")) is None:
            raise DBItemNotFound(key)
        return item

    # cant' do a @typechecked() since optional item requires a single type
    def delete_item(self, partition_key: str, partition_value: Union[str, int], sort_key: Union[str, None] = None, sort_value: Union[str, int, None] = None):
        """
        Delete table item

        :param partition_key: item partition (aka hash) key
        :param partition_value: item partition (aka hash) value
        :param sort_key: item sort key (if exists)
        :param sort_value: item sort value (if sort key exists)
        """
        table = self.resource.Table(self.table_name)
        key = {partition_key: partition_value}  # type: dict[str, Any]
        if sort_key is not None:
            key[sort_key] = sort_value
        table.delete_item(Key=key)

    # cant' do a @typechecked() since optional item requires a single type
    def upsert_item(self, partition_key: str, partition_value: Union[str, int], sort_key: Union[str, None] = None, sort_value: Union[str, int, None] = None, item: Union[dict, None] = None):

        """
        Upsert (update or insert) table item

        :param partition_key: item partition (aka hash) key
        :param partition_value: item partition (aka hash) value
        :param sort_key: item sort key (if exists)
        :param sort_value: item sort value (if sort key exists)
        :param item: item data
        """

        if item is None:
            AWSimpleException(f"{item=}")
        else:
            table = self.resource.Table(self.table_name)
            key = {partition_key: partition_value}  # type: dict[str, Any]
            if sort_key is not None:
                key[sort_key] = sort_value

            # create the required boto3 strings and dict for the update
            update_expression = "SET "
            expression_attribute_values = {}
            for k, v in item.items():
                update_expression += f"{k} = :{k} "
                expression_attribute_values[f":{k}"] = v

            table.update_item(Key=key, UpdateExpression=update_expression, ExpressionAttributeValues=expression_attribute_values)

    def delete_all_items(self) -> int:
        """
        Delete all the items in a table.

        Caution: since DynamoDB doesn't have a built-in mechanism to delete all items, items are deleted one at a time (we don't do a
        table delete/create since it's almost impossible to re-create all the indexes and potential references to other AWS resources).
        Therefore, executing this on large tables will take time and potentially cost money.  You may want to do a delete/create you can
        programmatically recreate the table and its references.

        (This is similar to a SQL "DELETE" with no WHERE clause.)

        :return: number of items deleted
        """
        table = self.resource.Table(self.table_name)
        hash_key, sort_key = self.get_primary_keys()
        count = 0
        for item in self.scan_table():
            key = {hash_key: item[hash_key]}
            if sort_key is not None:
                key[sort_key] = item[sort_key]
            table.delete_item(Key=key)
            count += 1
        return count
