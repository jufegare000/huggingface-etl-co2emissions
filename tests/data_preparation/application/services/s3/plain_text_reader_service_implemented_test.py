from typing import cast
from unittest.mock import MagicMock
import pytest

from data_preparation.application.services.s3.plain_text_reader_service_implemented import \
    PlainTextReaderServiceImplemented
from data_preparation.domain.models.s3.bucket_uri import BucketURI
from data_preparation.domain.services.s3.s3_parser_service import S3ParserService
from data_preparation.domain.services.s3.s3_service import S3Service

VAL_S3_URI = "s3://my-bucket/dataset.csv"
VAL_BUCKET = "my-bucket"
VAL_KEY = "dataset.csv"
KEY_BODY = "Body"

KEY_MODEL_ID = "model_id"
KEY_CO2_EQ = "co2_eq_emissions"

VAL_MODEL_1 = "model_1"
VAL_EMISSION_1 = "150.5"
VAL_MODEL_2 = "model_2"
VAL_EMISSION_2 = "300.2"

CSV_VALID = "model_id,co2_eq_emissions\nmodel_1,150.5\nmodel_2,300.2"
CSV_NO_HEADER = ""
CSV_MISSING_MODEL_ID = "co2_eq_emissions\n150.5"
CSV_MISSING_CO2_EQ = "model_id\nmodel_1"

ERR_NO_HEADER_PREFIX = "CSV has no header: "
ERR_MISSING_MODEL_ID = "CSV must contain model_id"
ERR_MISSING_CO2_EQ = "CSV must contain co2_eq_emissions"

INT_ZERO = 0
INT_ONE = 1
INT_TWO = 2


@pytest.fixture
def mock_s3_uri_service() -> MagicMock:
    return MagicMock(spec=S3ParserService)


@pytest.fixture
def mock_s3_service() -> MagicMock:
    return MagicMock(spec=S3Service)


@pytest.fixture
def mock_bucket_uri() -> MagicMock:
    bucket_uri = MagicMock(spec=BucketURI)
    bucket_uri.bucket = VAL_BUCKET
    bucket_uri.value = VAL_KEY
    return bucket_uri


@pytest.fixture
def service(
    mock_s3_uri_service, mock_s3_service
) -> PlainTextReaderServiceImplemented:
    return PlainTextReaderServiceImplemented(
        s3_uri_service=cast(S3ParserService, cast(object, mock_s3_uri_service)),
        s3_service=cast(S3Service, cast(object, mock_s3_service)),
    )


def test_read_csv_from_s3_success(
    service, mock_s3_uri_service, mock_s3_service, mock_bucket_uri
):
    mock_s3_uri_service.parse_s3_uri.return_value = mock_bucket_uri

    mock_response = MagicMock()
    mock_response.read.return_value = CSV_VALID.encode()

    mock_client = MagicMock()
    mock_client.get_object.return_value = {KEY_BODY: mock_response}
    mock_s3_service.get_client.return_value = mock_client

    result = service.read_csv_from_s3(VAL_S3_URI)

    assert len(result) == INT_TWO
    assert result[INT_ZERO][KEY_MODEL_ID] == VAL_MODEL_1
    assert result[INT_ZERO][KEY_CO2_EQ] == VAL_EMISSION_1
    assert result[INT_ONE][KEY_MODEL_ID] == VAL_MODEL_2
    assert result[INT_ONE][KEY_CO2_EQ] == VAL_EMISSION_2

    mock_s3_uri_service.parse_s3_uri.assert_called_once_with(VAL_S3_URI)
    mock_client.get_object.assert_called_once_with(
        Bucket=VAL_BUCKET, Key=VAL_KEY
    )


def test_read_csv_from_s3_no_header_raises_value_error(
    service, mock_s3_uri_service, mock_s3_service, mock_bucket_uri
):
    mock_s3_uri_service.parse_s3_uri.return_value = mock_bucket_uri

    mock_response = MagicMock()
    mock_response.read.return_value = CSV_NO_HEADER.encode()

    mock_client = MagicMock()
    mock_client.get_object.return_value = {KEY_BODY: mock_response}
    mock_s3_service.get_client.return_value = mock_client

    expected_error_message = ERR_NO_HEADER_PREFIX + VAL_S3_URI

    with pytest.raises(ValueError) as exc_info:
        service.read_csv_from_s3(VAL_S3_URI)

    assert str(exc_info.value) == expected_error_message


def test_read_csv_from_s3_missing_model_id_raises_value_error(
    service, mock_s3_uri_service, mock_s3_service, mock_bucket_uri
):
    mock_s3_uri_service.parse_s3_uri.return_value = mock_bucket_uri

    mock_response = MagicMock()
    mock_response.read.return_value = CSV_MISSING_MODEL_ID.encode()

    mock_client = MagicMock()
    mock_client.get_object.return_value = {KEY_BODY: mock_response}
    mock_s3_service.get_client.return_value = mock_client

    with pytest.raises(ValueError) as exc_info:
        service.read_csv_from_s3(VAL_S3_URI)

    assert str(exc_info.value) == ERR_MISSING_MODEL_ID


def test_read_csv_from_s3_missing_co2_eq_emissions_raises_value_error(
    service, mock_s3_uri_service, mock_s3_service, mock_bucket_uri
):
    mock_s3_uri_service.parse_s3_uri.return_value = mock_bucket_uri

    mock_response = MagicMock()
    mock_response.read.return_value = CSV_MISSING_CO2_EQ.encode()

    mock_client = MagicMock()
    mock_client.get_object.return_value = {KEY_BODY: mock_response}
    mock_s3_service.get_client.return_value = mock_client

    with pytest.raises(ValueError) as exc_info:
        service.read_csv_from_s3(VAL_S3_URI)

    assert str(exc_info.value) == ERR_MISSING_CO2_EQ