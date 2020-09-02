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

from appdirs import user_cache_dir
from boto3.exceptions import RetriesExceededError
from botocore.exceptions import EndpointConnectionError, ClientError
from typeguard import typechecked
from balsa import get_logger
from dictim import dictim

from awsimple import AWSAccess, __application_name__, __author__

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


@typechecked(always=True)
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


@typechecked(always=True)
def _is_valid_db_pickled_file(file_path: Path, cache_life: (float, int, None)) -> bool:
    is_valid = file_path.exists() and getsize(str(file_path)) > 0
    if is_valid and cache_life is not None:
        is_valid = time.time() <= getmtime(str(file_path)) + cache_life
    return is_valid


class DynamoDBAccess(AWSAccess):

    @typechecked(always=True)
    def __init__(self, table_name: str, **kwargs):
        self.table_name = table_name
        super().__init__(resource_name="dynamodb", **kwargs)

    @typechecked(always=True)
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

    @typechecked(always=True)
    def scan_table(self) -> (list, None):
        """
        returns entire lookup table
        :param table_name: DynamoDB table name
        :param profile_name: AWS IAM profile name
        :return: table contents
        """

        items = []
        dynamodb = self.resource
        table = dynamodb.Table(self.table_name)

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

    @typechecked(always=True)
    def scan_table_cached(self, invalidate_cache: bool = False) -> list:
        """

        Read data table(s) from AWS with caching.  This *requires* that the table not change during execution nor
        from run to run without setting invalidate_cache.

        :param invalidate_cache: True to initially invalidate the cache (forcing a table scan)
        :return: a list with the (possibly cached) table data
        """

        # todo: check the table size in AWS (since this is quick) and if it's different than what's in the cache, invalidate the cache first

        if self.cache_dir is None:
            self.cache_dir = Path(user_cache_dir(__application_name__, __author__), "aws", "dynamodb")
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file_path = Path(self.cache_dir, f"{self.table_name}.pickle")
        log.debug(f"cache_file_path : {cache_file_path.resolve()}")
        if invalidate_cache and cache_file_path.exists():
            cache_file_path.unlink(missing_ok=True)

        output_data = []
        if _is_valid_db_pickled_file(cache_file_path, self.cache_life):
            with open(cache_file_path, "rb") as f:
                log.info(f"{self.table_name} : reading {cache_file_path}")
                output_data = pickle.load(f)
                log.debug(f"done reading {cache_file_path}")

        if not _is_valid_db_pickled_file(cache_file_path, self.cache_life):
            log.info(f"getting {self.table_name} from DB")

            try:
                table_data = self.scan_table()
            except RetriesExceededError:
                table_data = None

            if table_data is not None and len(table_data) > 0:
                output_data = table_data
                # update data cache
                with open(cache_file_path, "wb") as f:
                    pickle.dump(output_data, f)

        if output_data is None:
            log.error(f'table "{self.table_name}" not accessible')

        return output_data

    @typechecked(always=True)
    def create_table(self, partition_key: str, sort_key: str = None) -> bool:
        def add_key(k, t, kt):
            assert t in ("S", "N", "B")  # DynamoDB key types (string, number, byte)
            assert kt in ("HASH", "RANGE")
            _d = {"AttributeName": k, "AttributeType": t}
            _s = {"AttributeName": k, "KeyType": kt}
            return _d, _s

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

            d, s = add_key(partition_key, type_to_attribute_type(partition_key), "HASH")  # required
            attribute_definitions = [d]
            key_schema = [s]
            if sort_key is not None:
                d, s = add_key(sort_key, type_to_attribute_type(sort_key), "RANGE")  # optional
                attribute_definitions.append(d)
                key_schema.append(s)
            log.info(pformat(key_schema, indent=4))

            try:
                client.create_table(AttributeDefinitions=attribute_definitions, KeySchema=key_schema, BillingMode="PAY_PER_REQUEST", TableName=self.table_name)  # on-demand
                created = True
            except ClientError as e:
                log.warning(e)

        return created

    @typechecked(always=True)
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
                deleted_it = True
                done = True
            except self.client.exceptions.ResourceInUseException:
                time.sleep(10)
            except self.client.exceptions.ResourceNotFoundException:
                done = True
            timeout_count -= 1
        return deleted_it

    @typechecked(always=True)
    def table_exists(self) -> bool:
        assert self.table_name is not None
        try:
            self.client.describe_table(TableName=self.table_name)
            table_exists = True
        except self.client.exceptions.ResourceNotFoundException:
            table_exists = False
        return table_exists

    @typechecked(always=True)
    def put_item(self, item: dict):
        table = self.resource.Table(self.table_name)
        table.put_item(Item=item)

    def get_item(self, partition_key: str, partition_value: (str, int), sort_key: str = None, sort_value: (str, int) = None) -> (dict, None):
        table = self.resource.Table(self.table_name)
        key = {partition_key: partition_value}
        if sort_key is not None:
            key[sort_key] = sort_value
        response = table.get_item(Key=key)
        if (item := response.get('Item')) is None:
            log.warning(f"{self.table_name=} {key=} does not exist")
        return item

    def delete_item(self, partition_key: str, partition_value: (str, int), sort_key: str = None, sort_value: (str, int) = None):
        table = self.resource.Table(self.table_name)
        key = {partition_key: partition_value}
        if sort_key is not None:
            key[sort_key] = sort_value
        table.delete_item(Key=key)

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
