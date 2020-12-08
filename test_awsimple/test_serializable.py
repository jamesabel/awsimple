from decimal import Decimal
from enum import Enum

from awsimple import dynamodb_to_dict


class TstClass(Enum):
    a = 1
    b = 2


def test_make_serializable():
    values = {"d": Decimal(1.0), "s": "s", "bool": True, "a": TstClass.a, "b": TstClass.b}
    serial_values = dynamodb_to_dict(values)
    assert serial_values["d"] == 1.0
    assert serial_values["s"] == "s"
    assert serial_values["bool"] is True
    assert serial_values["a"] == 1
    assert serial_values["b"] == 2
