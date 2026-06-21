from decimal import Decimal

import pytest

from application.services.dynamodb.dynamodb_conversion_service_implemented import (
    DynamoDBTypeConversionServiceImplemented
)


@pytest.fixture
def service() -> DynamoDBTypeConversionServiceImplemented:
    return DynamoDBTypeConversionServiceImplemented()


def test_convert_float_to_decimal(service: DynamoDBTypeConversionServiceImplemented):
    result = service.convert_floats_to_decimal(12.45)

    assert result == Decimal("12.45")
    assert isinstance(result, Decimal)


def test_convert_floats_inside_dict(service: DynamoDBTypeConversionServiceImplemented):
    value = {
        "model_id": "bert-base",
        "co2": 12.45,
        "count": 100,
    }

    result = service.convert_floats_to_decimal(value)

    assert result == {
        "model_id": "bert-base",
        "co2": Decimal("12.45"),
        "count": 100,
    }


def test_convert_nested_dict_and_list(service: DynamoDBTypeConversionServiceImplemented):
    value = {
        "model_id": "bert-base",
        "emissions": {
            "co2_kg": 12.45,
            "confidence": 0.91,
        },
        "scores": [0.1, 0.2, {"f1": 0.87}],
    }

    result = service.convert_floats_to_decimal(value)

    assert result == {
        "model_id": "bert-base",
        "emissions": {
            "co2_kg": Decimal("12.45"),
            "confidence": Decimal("0.91"),
        },
        "scores": [
            Decimal("0.1"),
            Decimal("0.2"),
            {"f1": Decimal("0.87")},
        ],
    }