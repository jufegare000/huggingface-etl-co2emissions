import pytest
from data_preparation.domain.models.s3.bucket_uri import BucketURI
from shared.application.services.s3.s3_parser_service_implemented import S3ParserServiceImplemented

VAL_VALID_URI = "s3://my-bucket/folder/data.csv"
VAL_EXPECTED_BUCKET = "my-bucket"
VAL_EXPECTED_KEY = "folder/data.csv"

VAL_INVALID_URI = "https://my-bucket/folder/data.csv"
ERR_MSG_PREFIX = "Expected s3 URI, got: "

VAL_ROOT_URI = "s3://another-bucket/file.json"
VAL_EXPECTED_ROOT_BUCKET = "another-bucket"
VAL_EXPECTED_ROOT_KEY = "file.json"


@pytest.fixture
def service() -> S3ParserServiceImplemented:
    return S3ParserServiceImplemented()


def test_parse_s3_uri_success(service):
    result = service.parse_s3_uri(VAL_VALID_URI)

    assert isinstance(result, BucketURI)
    assert result.bucket == VAL_EXPECTED_BUCKET
    assert result.value == VAL_EXPECTED_KEY


def test_parse_s3_uri_root_file_success(service):
    result = service.parse_s3_uri(VAL_ROOT_URI)

    assert result.bucket == VAL_EXPECTED_ROOT_BUCKET
    assert result.value == VAL_EXPECTED_ROOT_KEY


def test_parse_s3_uri_invalid_scheme_raises_value_error(service):
    expected_error_message = ERR_MSG_PREFIX + VAL_INVALID_URI

    with pytest.raises(ValueError) as exc_info:
        service.parse_s3_uri(VAL_INVALID_URI)

    assert str(exc_info.value) == expected_error_message