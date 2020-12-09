from decimal import Decimal
from enum import Enum
from pathlib import Path

from PIL import Image

from awsimple import dict_to_dynamodb, dynamodb_to_dict


class TstClass(Enum):
    a = 1
    b = 2


def test_make_serializable():
    values = {"d": Decimal(1.0), "s": "s", "bool": True, "a": TstClass.a, "b": TstClass.b, "binary": b"\0\1"}
    values["image"] = Image.open(Path("test_awsimple", "280px-PNG_transparency_demonstration_1.png"))
    values = dict_to_dynamodb(values)
    serial_values = dynamodb_to_dict(values)
    assert serial_values["d"] == 1.0
    assert serial_values["s"] == "s"
    assert serial_values["bool"] is True
    assert serial_values["a"] == "a"
    assert serial_values["b"] == "b"
    assert len(serial_values["image"]) == 140065
    assert serial_values["binary"] == "b'\\x00\\x01'"
