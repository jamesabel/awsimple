import io
import decimal
import pickle
import time
from collections import OrderedDict, defaultdict
import datetime
from pathlib import Path
from os.path import getsize, getmtime
from typing import List
from pprint import pformat
from itertools import islice
import os

from appdirs import user_cache_dir
from boto3.exceptions import RetriesExceededError
from botocore.exceptions import EndpointConnectionError, ClientError
from boto3.dynamodb.conditions import Key
from typeguard import typechecked
from balsa import get_logger
from dictim import dictim

from awsimple import AWSAccess, __application_name__, __author__, AWSimpleException

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

log = get_logger(__application_name__)


@typechecked()
def dict_to_dynamodb(input_value, convert_images: bool = True, raise_exception: bool = True):
    """
    makes a dictionary follow boto3 item standards

    :param input_value: input dictionary
    :param convert_images: set to False to skip over images (they can be too large)
    :param raise_exception: set to False to not raise exceptions on issues

    :return: converted version of the original dictionary

    """
    resp = None
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
def _is_valid_db_pickled_file(file_path: Path, cache_life: (float, int, None)) -> bool:
    is_valid = file_path.exists() and getsize(str(file_path)) > 0
    if is_valid and cache_life is not None:
        is_valid = time.time() <= getmtime(str(file_path)) + cache_life
    return is_valid


class DynamoDBAccess(AWSAccess):
    @typechecked()
    def __init__(self, table_name: str = None, **kwargs):
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

    @typechecked()
    def scan_table(self) -> (list, None):
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
                items = None

            if response is not None:
                items.extend(response["Items"])
                if "LastEvaluatedKey" not in response:
                    more_to_evaluate = False
                else:
                    exclusive_start_key = response["LastEvaluatedKey"]

        if items is not None:
            log.info(f"read {len(items)} items from {self.table_name}")

        return items

    @typechecked()
    def scan_table_cached(self, invalidate_cache: bool = False) -> (list, None):
        """

        Read data table(s) from AWS with caching.  This *requires* that the table not change during execution nor
        from run to run without setting invalidate_cache.

        :param invalidate_cache: True to initially invalidate the cache (forcing a table scan)
        :return: a list with the (possibly cached) table data
        """

        if self.cache_dir is None:
            self.cache_dir = Path(user_cache_dir(__application_name__, __author__), "aws", "dynamodb")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file_path = Path(self.cache_dir, f"{self.table_name}.pickle")
        log.debug(f"cache_file_path : {cache_file_path.resolve()}")

        if invalidate_cache:
            cache_file_path.unlink(missing_ok=True)

        table_data = None
        if _is_valid_db_pickled_file(cache_file_path, self.cache_life):
            with open(cache_file_path, "rb") as f:

                log.info(f"{self.table_name=} : reading {cache_file_path=}")
                table_data = pickle.load(f)
                log.debug(f"done reading {cache_file_path=}")

                # AWS updates DynamoDB item count approximately every 6 hours
                cache_age = time.time() - os.path.getmtime(cache_file_path)
                item_count_mismatch = cache_age > datetime.timedelta(hours=6).total_seconds() and self.resource.Table(self.table_name).item_count != len(table_data)

        if table_data is None or item_count_mismatch:
            log.info(f"getting {self.table_name} from DB")

            try:
                table_data = self.scan_table()
            except RetriesExceededError:
                table_data = None

            if table_data is not None and len(table_data) > 0:
                # update data cache
                with open(cache_file_path, "wb") as f:
                    pickle.dump(table_data, f)

            self.cache_hit = False
        else:
            self.cache_hit = True

        if table_data is None:
            log.error(f'table "{self.table_name}" not accessible')

        return table_data

    @typechecked()
    def create_table(self, partition_key: str, sort_key: str = None, secondary_index: str = None) -> bool:
        def add_key(k, t, kt):
            assert t in ("S", "N", "B")  # DynamoDB key types (string, number, byte)
            assert kt in ("HASH", "RANGE")
            definition = {"AttributeName": k, "AttributeType": t}
            schema = {"AttributeName": k, "KeyType": kt}
            return definition, schema

        def type_to_attribute_type(v):
            if isinstance(v, str):
                t = "S"
            elif isinstance(v, int):
                t = "N"
            elif isinstance(v, bytes):
                t = "B"
            else:
                raise ValueError(type(v), v)
            return t

        created = False
        if not self.table_exists():
            client = self.client

            # https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html#HowItWorks.CoreComponents.PrimaryKey

            partition_definition, partition_schema = add_key(partition_key, type_to_attribute_type(partition_key), "HASH")  # required
            attribute_definitions = [partition_definition]
            key_schema = [partition_schema]
            if sort_key is not None:
                sort_definition, sort_schema = add_key(sort_key, type_to_attribute_type(sort_key), "RANGE")  # optional
                attribute_definitions.append(sort_definition)
                key_schema.append(sort_schema)
            log.info(pformat(key_schema, indent=4))

            kwargs = {"AttributeDefinitions": attribute_definitions,
                      "KeySchema": key_schema,
                      "BillingMode": "PAY_PER_REQUEST",  # on-demand
                      "TableName": self.table_name
                      }

            # add a secondary index, if requested
            if secondary_index is not None:
                index_name = f"{secondary_index}{self.secondary_index_postfix}"
                # currently we only support a single index key (thus the HASH type)
                secondary_definition, secondary_schema = add_key(secondary_index, type_to_attribute_type(sort_key), "HASH")
                # global secondary index does not required the secondary index to be of the same form as the primary
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
        :return: a 2-tuple of (primary_key, sort_key).  sort_key will be None if there is no sort key.
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
            query(primary_key, primary_value)
            query(primary_key, primary_value, sort_key, sort_value)

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

        kwargs = {"KeyConditionExpression": key_condition_expression}
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
    def delete_table(self) -> bool:
        """
        deletes the current table
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
        assert self.table_name is not None
        try:
            self.client.describe_table(TableName=self.table_name)
            table_exists = True
        except self.client.exceptions.ResourceNotFoundException:
            table_exists = False
        return table_exists

    @typechecked()
    def put_item(self, item: dict):
        table = self.resource.Table(self.table_name)
        table.put_item(Item=item)

    # cant' do a @typechecked() since optional item requires a single type
    def get_item(self, partition_key: str, partition_value: (str, int), sort_key: str = None, sort_value: (str, int) = None) -> dict:
        """
        get a DB item
        :param partition_key: partition key
        :param partition_value: partition value (str or int)
        :param sort_key: sort key (optional)
        :param sort_value: sort value (optional str or int)
        :return: item dict or raises DBItemNotFound if does not exist
        """
        table = self.resource.Table(self.table_name)
        key = {partition_key: partition_value}
        if sort_key is not None:
            key[sort_key] = sort_value
        response = table.get_item(Key=key)
        if (item := response.get("Item")) is None:
            raise DBItemNotFound(key)
        return item

    # cant' do a @typechecked() since optional item requires a single type
    def delete_item(self, partition_key: str, partition_value: (str, int), sort_key: str = None, sort_value: (str, int) = None):
        table = self.resource.Table(self.table_name)
        key = {partition_key: partition_value}
        if sort_key is not None:
            key[sort_key] = sort_value
        table.delete_item(Key=key)

    # cant' do a @typechecked() since optional item requires a single type
    def upsert_item(self, partition_key: str, partition_value: (str, int), sort_key: str = None, sort_value: (str, int) = None, item: dict = None):

        if item is None:
            log.warning(f"{item=} : nothing to do")
        else:

            table = self.resource.Table(self.table_name)
            key = {partition_key: partition_value}
            if sort_key is not None:
                key[sort_key] = sort_value

            # create the required boto3 strings and dict for the update
            update_expression = "SET "
            expression_attribute_values = {}
            for k, v in item.items():
                update_expression += f"{k} = :{k} "
                expression_attribute_values[f":{k}"] = v

            table.update_item(Key=key, UpdateExpression=update_expression, ExpressionAttributeValues=expression_attribute_values)
