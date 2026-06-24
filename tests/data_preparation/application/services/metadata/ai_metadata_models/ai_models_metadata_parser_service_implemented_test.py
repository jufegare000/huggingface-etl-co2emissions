import pytest
from data_preparation.application.services.metadata.ai_metadata_models.ai_models_metadata_parser_service_implemented import (
    AIModelsMetadataParserServiceImplemented,
)

VAL_NONE = None
VAL_STR_SPACED_FLOAT = "  12.34  "
VAL_STR_INT = "100"
VAL_STR_INVALID = "not_a_number"
VAL_FLOAT_NUM = 5.67
VAL_INT_NUM = 88
VAL_STR_FLOAT_FOR_INT = "  42.99  "

FLT_EXPECTED_12_34 = 12.34
FLT_EXPECTED_100 = 100.0
FLT_EXPECTED_5_67 = 5.67

INT_EXPECTED_0 = 0
INT_EXPECTED_12 = 12
INT_EXPECTED_100 = 100
INT_EXPECTED_88 = 88
INT_EXPECTED_42 = 42


@pytest.fixture
def service() -> AIModelsMetadataParserServiceImplemented:
    return AIModelsMetadataParserServiceImplemented()


@pytest.mark.parametrize(
    "test_input,expected_output",
    [
        (VAL_NONE, VAL_NONE),
        (VAL_STR_SPACED_FLOAT, FLT_EXPECTED_12_34),
        (VAL_STR_INT, FLT_EXPECTED_100),
        (VAL_STR_INVALID, VAL_NONE),
        (VAL_FLOAT_NUM, FLT_EXPECTED_5_67),
    ],
)
def test_safe_float(service, test_input, expected_output):
    assert service.safe_float(test_input) == expected_output


@pytest.mark.parametrize(
    "test_input,expected_output",
    [
        (VAL_NONE, INT_EXPECTED_0),
        (VAL_STR_SPACED_FLOAT, INT_EXPECTED_12),
        (VAL_STR_INT, INT_EXPECTED_100),
        (VAL_STR_INVALID, INT_EXPECTED_0),
        (VAL_INT_NUM, INT_EXPECTED_88),
        (VAL_STR_FLOAT_FOR_INT, INT_EXPECTED_42),
    ],
)
def test_safe_int(service, test_input, expected_output):
    assert service.safe_int(test_input) == expected_output