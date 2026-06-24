from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum

from shared.domain.models.serializable_model import SerializableModel

VAL_ENUM_VAL = "ACTIVE"
KEY_NAME = "name"
VAL_NAME = "Alice"
KEY_AGE = "age"
INT_AGE = 25
KEY_STATUS = "status"
KEY_CREATED_AT = "created_at"
KEY_PRICE = "price"

INT_YEAR = 2026
INT_MONTH = 6
INT_DAY = 22
INT_HOUR = 12
INT_MINUTE = 0
VAL_ISO_DATE = "2026-06-22T12:00:00"

STR_DECIMAL = "19.99"
FLT_DECIMAL = 19.99

VAL_STR_ELEMENT = "item"
VAL_PRIMITIVE_INT = 42
VAL_PRIMITIVE_STR = "hello"

INT_ZERO = 0
INT_ONE = 1


class DummyEnum(Enum):
    ACTIVE = VAL_ENUM_VAL


@dataclass
class DummyDataclass:
    name: str
    age: int


class ComplexModel(SerializableModel):
    def __init__(self, name: str, status: DummyEnum, created_at: datetime, price: Decimal):
        self.name = name
        self.status = status
        self.created_at = created_at
        self.price = price


def test_serialize_primitive_types():
    model = SerializableModel()

    assert model._serialize(VAL_PRIMITIVE_INT) == VAL_PRIMITIVE_INT
    assert model._serialize(VAL_PRIMITIVE_STR) == VAL_PRIMITIVE_STR


def test_serialize_enum():
    model = SerializableModel()

    result = model._serialize(DummyEnum.ACTIVE)

    assert result == VAL_ENUM_VAL


def test_serialize_datetime():
    model = SerializableModel()
    dt = datetime(INT_YEAR, INT_MONTH, INT_DAY, INT_HOUR, INT_MINUTE)

    result = model._serialize(dt)

    assert result == VAL_ISO_DATE


def test_serialize_decimal():
    model = SerializableModel()
    dec = Decimal(STR_DECIMAL)

    result = model._serialize(dec)

    assert isinstance(result, float)
    assert result == FLT_DECIMAL


def test_serialize_dataclass():
    model = SerializableModel()
    dc = DummyDataclass(name=VAL_NAME, age=INT_AGE)

    result = model._serialize(dc)

    assert isinstance(result, dict)
    assert result[KEY_NAME] == VAL_NAME
    assert result[KEY_AGE] == INT_AGE


def test_serialize_list():
    model = SerializableModel()
    data_list = [DummyEnum.ACTIVE, VAL_STR_ELEMENT]

    result = model._serialize(data_list)

    assert isinstance(result, list)
    assert len(result) == len(data_list)
    assert result[INT_ZERO] == VAL_ENUM_VAL
    assert result[INT_ONE] == VAL_STR_ELEMENT


def test_serialize_dict():
    model = SerializableModel()
    data_dict = {KEY_STATUS: DummyEnum.ACTIVE, KEY_NAME: VAL_NAME}

    result = model._serialize(data_dict)

    assert isinstance(result, dict)
    assert result[KEY_STATUS] == VAL_ENUM_VAL
    assert result[KEY_NAME] == VAL_NAME


def test_to_dict_complex_model():
    dt = datetime(INT_YEAR, INT_MONTH, INT_DAY, INT_HOUR, INT_MINUTE)
    dec = Decimal(STR_DECIMAL)
    model = ComplexModel(
        name=VAL_NAME,
        status=DummyEnum.ACTIVE,
        created_at=dt,
        price=dec
    )

    result = model.to_dict()

    assert isinstance(result, dict)
    assert result[KEY_NAME] == VAL_NAME
    assert result[KEY_STATUS] == VAL_ENUM_VAL
    assert result[KEY_CREATED_AT] == VAL_ISO_DATE
    assert result[KEY_PRICE] == FLT_DECIMAL