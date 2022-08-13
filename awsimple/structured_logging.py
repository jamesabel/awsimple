# "vendored" from balsa to eliminate AWSimple's dependency on balsa so that AWSimple can use balsa without circular import worries.

import decimal
from datetime import datetime
from decimal import Decimal
from enum import Enum
import re
import logging
import json

import dateutil.parser

from logging import getLogger

from awsimple import __application_name__

log = getLogger(__application_name__)


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
        serializable_representation = str(o.value)
    else:
        serializable_representation = str(o)
        # raise NotImplementedError(f"can not serialize {o} since type={type(o)}")
    return serializable_representation


structured_sentinel = "<>"  # illegal JSON


def sf(*args, **kwargs):
    """
    Structured formatter helper function. When called with any number of positional or keyword arguments, creates a structured string representing those arguments.
    This is a short function name (sf) since it usually goes inside a logging call.

    Example code:
    question = "life"
    answer = 42
    log.info(sf("test structured logging", question=question, answer=answer))

    log:
    2021-10-24T10:38:54.524721-07:00 - test_structured_logging - test_structured_logging.py - 16 - test_to_structured_logging - INFO - test structured logging <> {"question": "life", "answer": 42} <>

    :param args: args
    :param kwargs: kwargs
    """

    separator = ","
    output_list = []
    if len(args) > 0:
        output_list.append(separator.join(args))
    if len(kwargs) > 0:
        # use json.dumps to handle special strings (e.g. embedded quotes)
        output_list.extend([structured_sentinel, json.dumps(kwargs, default=convert_serializable_special_cases), structured_sentinel])
    return " ".join(output_list)


balsa_log_regex = re.compile(r"([0-9\-:T.]+) - ([\S]+) - ([\S]+) - ([0-9]+) - ([\S]+) - (NOTSET|DEBUG|INFO|WARN|WARNING|ERROR|FATAL|CRITICAL) - (.*)", flags=re.IGNORECASE | re.DOTALL)


class BalsaRecord:
    """
    Balsa log record as a class.
    """

    time_stamp: datetime
    name: str
    file_name: str
    line_number: int
    function_name: str
    log_level: int  # e.g. logging.INFO, etc. since levels are internally stored as integers
    message: str
    structured_record: dict

    def __init__(self, log_string: str):
        """
        Convert log string to Balsa record.
        :param log_string: log string
        """
        if (groups := balsa_log_regex.match(log_string)) is None:
            self.time_stamp = datetime.now()
            self.name = ""
            self.file_name = ""
            self.line_number = 0
            self.function_name = ""
            self.log_level = logging.NOTSET
            self.message = ""
            self.structured_record = {}
        else:
            self.time_stamp = dateutil.parser.parse(groups.group(1))
            self.name = groups.group(2)
            self.file_name = groups.group(3)
            self.line_number = int(groups.group(4))
            self.function_name = groups.group(5)
            self.log_level = getattr(logging, groups.group(6))  # log level as an integer value

            self.structured_record = {}
            structured_string = groups.group(7).strip()
            if structured_string.endswith(structured_sentinel) and (start_structured_string := structured_string.find(structured_sentinel)) >= 0:
                start_json = start_structured_string + len(structured_sentinel) + 1
                json_string = structured_string[start_json : -len(structured_sentinel)]
                self.message = structured_string[:start_json]
                try:
                    self.structured_record = json.loads(json_string)
                except json.JSONDecodeError:
                    log.warning(f"could not JSON decode : {json_string}")
                    self.message += f" {structured_sentinel} {json_string} {structured_sentinel}"  # fallback if we can't decode the JSON, at least have it as part of the message string
            else:
                self.message = structured_string  # no JSON part

    def __repr__(self):
        """
        Create a log string from this object. Balsa's structured logs are invertible, i.e. you can give BalsaRecord a log string and then this repr will produce the original string.
        :return: log string
        """
        log_level = logging.getLevelName(self.log_level)
        fields = [self.time_stamp.astimezone().isoformat(), self.name, self.file_name, str(self.line_number), self.function_name, log_level]

        structured_string = ""
        if len(self.message) > 0:
            structured_string = self.message
        if len(self.structured_record) > 0:
            json_string = json.dumps(self.structured_record)
            structured_string += f"{json_string} {structured_sentinel}"
        if len(structured_string) > 0:
            fields.append(structured_string)

        output_string = " - ".join(fields)
        return output_string
