from typing import cast
from unittest.mock import MagicMock, patch
import pytest

from data_preparation.domain.models.preparation.input_manifest import InputManifest
from data_preparation.application.services.config.lambda_function.data_preparation_enum import (
    DataPreparationConfigParams,
)
from shared.domain.services.date_parsing.date_parsing_service import DataParsingService
from data_preparation.domain.services.config.lambda_function.lambda_config_validator_service import (
    LambdaConfigValidatorService,
)
from data_preparation.application.services.config.lambda_function.lambda_config_service_implemented import (
    LambdaConfigServiceImplemented,
)

PATCH_ENV_SERVICE = "data_preparation.application.services.config.lambda_function.lambda_config_service_implemented.env_service"
PATCH_CONFIG_PARAMS = "data_preparation.application.services.config.lambda_function.lambda_config_service_implemented.DataPreparationConfigParams"

ENV_RAW_BUCKET_NAME = "RAW_BUCKET_NAME"
ENV_CONTROL_TABLE_NAME = "CONTROL_TABLE_NAME"

KEY_RUN_ID = "run_id"
KEY_SOURCE_CSV_PATH = "source_csv_path"
KEY_WORKERS = "workers"
KEY_THREADS_PER_WORKER = "threads_per_worker"
KEY_PREPARED_PREFIX = "prepared_prefix"
KEY_GLOBAL_RATE_LIMIT = "global_rate_limit"
KEY_WINDOW_SECONDS = "window_seconds"
KEY_CALLS_PER_MODEL = "calls_per_model"

VAL_MOCK_BUCKET = "mock-raw-bucket"
VAL_MOCK_TABLE = "mock-control-table"
VAL_MOCK_COMPACT_NOW = "20260622110000"
VAL_DEFAULT_DISCOVERY_KEY = "default_discovery.csv"
VAL_DEFAULT_PREPARED_PREFIX = "prepared-prefix-root"

INT_DEFAULT_GLOBAL_LIMIT = 100
INT_DEFAULT_WINDOW_SECONDS = 60
INT_DEFAULT_CALLS_PER_MODEL = 10
INT_FALLBACK_WORKERS = 4
INT_FALLBACK_THREADS = 1

VAL_CUSTOM_RUN_ID = "custom-run-123"
VAL_CUSTOM_SOURCE_PATH = "s3://custom/path.csv"
VAL_CUSTOM_PREPARED_PREFIX = "s3://custom/prepared"
VAL_CUSTOM_WORKERS_STR = "8"
INT_CUSTOM_WORKERS = 8
VAL_CUSTOM_THREADS_STR = "2"
INT_CUSTOM_THREADS = 2
VAL_CUSTOM_GLOBAL_LIMIT_STR = "500"
INT_CUSTOM_GLOBAL_LIMIT = 500
VAL_CUSTOM_WINDOW_STR = "120"
INT_CUSTOM_WINDOW = 120
VAL_CUSTOM_CALLS_STR = "5"
INT_CUSTOM_CALLS = 5

ERROR_CONTROL_TABLE_REQUIRED = (
    "CONTROL_TABLE_NAME environment variable is required"
)
EMPTY_STRING = ""

EXPECTED_DEFAULT_S3_PATH = "s3://mock-raw-bucket/default_discovery.csv"
EXPECTED_DEFAULT_PREPARED_PATH = (
    "prepared-prefix-root/run_id=20260622110000"
)


@pytest.fixture
def mock_data_parsing_service() -> MagicMock:
    mock = MagicMock(spec=DataParsingService)
    mock.utc_now_compact.return_value = VAL_MOCK_COMPACT_NOW
    return mock


@pytest.fixture
def mock_lambda_config_validator() -> MagicMock:
    return MagicMock(spec=LambdaConfigValidatorService)


@pytest.fixture
def service(mock_data_parsing_service, mock_lambda_config_validator):
    return LambdaConfigServiceImplemented(
        data_parsing_service=cast(
            DataParsingService, cast(object, mock_data_parsing_service)
        ),
        lambda_config_validator=cast(
            LambdaConfigValidatorService, cast(object, mock_lambda_config_validator)
        ),
    )


@pytest.fixture
def mock_config_enum_params():
    with patch.object(
        DataPreparationConfigParams,
        "DISCOVERY_DEFAULT_KEY",
        VAL_DEFAULT_DISCOVERY_KEY,
    ), patch.object(
        DataPreparationConfigParams,
        "PREPARED_PREFIX",
        VAL_DEFAULT_PREPARED_PREFIX,
    ), patch.object(
        DataPreparationConfigParams,
        "GLOBAL_RATE_LIMIT",
        INT_DEFAULT_GLOBAL_LIMIT,
    ), patch.object(
        DataPreparationConfigParams,
        "WINDOW_SECONDS",
        INT_DEFAULT_WINDOW_SECONDS,
    ), patch.object(
        DataPreparationConfigParams,
        "CALLS_PER_MODEL",
        INT_DEFAULT_CALLS_PER_MODEL,
    ):
        yield


def test_load_input_manifest_executes_validation(
    service, mock_lambda_config_validator, mock_config_enum_params
):
    with patch(PATCH_ENV_SERVICE) as mock_env:
        mock_env.load_env_variable.side_effect = lambda var: (
            VAL_MOCK_BUCKET
            if var == ENV_RAW_BUCKET_NAME
            else (VAL_MOCK_TABLE if var == ENV_CONTROL_TABLE_NAME else None)
        )

        result = service.load_input_manifest()

        assert isinstance(result, InputManifest)
        mock_lambda_config_validator.validate_input.assert_called_once_with(
            result
        )


def test_create_manifest_structure_with_fallback_defaults(
    service, mock_config_enum_params
):
    with patch(PATCH_ENV_SERVICE) as mock_env:
        mock_env.load_env_variable.side_effect = lambda var: (
            VAL_MOCK_BUCKET
            if var == ENV_RAW_BUCKET_NAME
            else (VAL_MOCK_TABLE if var == ENV_CONTROL_TABLE_NAME else None)
        )

        manifest = service.create_manifest_structure()

        assert manifest.run_id == VAL_MOCK_COMPACT_NOW
        assert manifest.source_csv_path == EXPECTED_DEFAULT_S3_PATH
        assert manifest.workers == INT_FALLBACK_WORKERS
        assert manifest.threads_per_worker == INT_FALLBACK_THREADS
        assert manifest.bucket_name == VAL_MOCK_BUCKET
        assert manifest.control_table_name == VAL_MOCK_TABLE
        assert manifest.prepared_prefix == EXPECTED_DEFAULT_PREPARED_PATH
        assert manifest.global_rate_limit == INT_DEFAULT_GLOBAL_LIMIT
        assert manifest.window_seconds == INT_DEFAULT_WINDOW_SECONDS
        assert manifest.calls_per_model == INT_DEFAULT_CALLS_PER_MODEL



def test_create_manifest_structure_missing_table_name_raises_value_error(
    service,
):
    with patch(PATCH_ENV_SERVICE) as mock_env:
        mock_env.load_env_variable.side_effect = lambda var: (
            VAL_MOCK_BUCKET if var == ENV_RAW_BUCKET_NAME else None
        )

        with pytest.raises(ValueError) as exc_info:
            service.create_manifest_structure()

        assert str(exc_info.value) == ERROR_CONTROL_TABLE_REQUIRED


def test_create_manifest_structure_empty_bucket_name_fallback(
    service, mock_config_enum_params
):
    with patch(PATCH_ENV_SERVICE) as mock_env:
        mock_env.load_env_variable.side_effect = lambda var: (
            None
            if var == ENV_RAW_BUCKET_NAME
            else (VAL_MOCK_TABLE if var == ENV_CONTROL_TABLE_NAME else None)
        )

        manifest = service.create_manifest_structure()

        assert manifest.bucket_name == EMPTY_STRING