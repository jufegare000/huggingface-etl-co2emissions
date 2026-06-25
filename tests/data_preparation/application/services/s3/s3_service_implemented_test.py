from unittest.mock import MagicMock
import pytest

from shared.application.services.s3.s3_writer_service_implemented import S3WriterServiceImplemented

PATCH_CSV_COLUMNS = "data_preparation.application.services.s3.s3_service_implemented.CSV_COLUMNS"

VAL_BUCKET = "test-bucket"
VAL_KEY_CSV = "data/output.csv"
VAL_KEY_JSON = "data/output.json"
VAL_CONTENT_TYPE_CSV = "text/csv"
VAL_CONTENT_TYPE_JSON = "application/json"

COL_A = "col_a"
COL_B = "col_b"
KEY_EXTRA = "col_extra"

VAL_ROW1_A = "val1"
VAL_ROW1_B = "val2"
VAL_ROW2_A = "val3"
VAL_ROW2_B = "val4"
VAL_EXTRA_VAL = "ignored"

KEY_JSON_NAME = "name"
VAL_JSON_NAME = "test"
KEY_JSON_NUM = "number"
INT_JSON_NUM = 123

VAL_EXPECTED_CSV_BODY = b"col_a,col_b\r\nval1,val2\r\nval3,val4\r\n"
VAL_EXPECTED_JSON_BODY = b'{\n  "name": "test",\n  "number": 123\n}'


@pytest.fixture
def mock_s3_client():
    return MagicMock()


@pytest.fixture
def service(mock_s3_client):
    impl = S3WriterServiceImplemented()
    impl.s3_client = mock_s3_client
    return impl


def test_get_client(service, mock_s3_client):
    assert service.get_client() == mock_s3_client



def test_write_json_to_s3(service, mock_s3_client):
    payload = {KEY_JSON_NAME: VAL_JSON_NAME, KEY_JSON_NUM: INT_JSON_NUM}

    service.write_json_to_s3(payload, VAL_BUCKET, VAL_KEY_JSON)

    mock_s3_client.put_object.assert_called_once_with(
        Bucket=VAL_BUCKET,
        Key=VAL_KEY_JSON,
        Body=VAL_EXPECTED_JSON_BODY,
        ContentType=VAL_CONTENT_TYPE_JSON,
    )