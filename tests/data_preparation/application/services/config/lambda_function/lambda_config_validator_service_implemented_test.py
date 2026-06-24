from typing import cast
from unittest.mock import MagicMock
import pytest

from data_preparation.domain.models.preparation.input_manifest import InputManifest
from data_preparation.application.services.config.lambda_function.lambda_config_validator_service_implemented import (
    LambdaConfigValidatorServiceImplemented,
)

ERR_WORKERS = "workers must be > 0"
ERR_THREADS = "threads_per_worker must be > 0"
ERR_RATE_LIMIT = "global_rate_limit must be > 0"
ERR_WINDOW = "window_seconds must be > 0"
ERR_CALLS = "calls_per_model must be > 0"
ERR_PATH_REQUIRED = "source_csv_path is required"

VAL_VALID_PATH = "s3://bucket/dataset.csv"
VAL_EMPTY_PATH = ""

INT_VALID_WORKERS = 4
INT_VALID_THREADS = 1
INT_VALID_LIMIT = 100
INT_VALID_WINDOW = 60
INT_VALID_CALLS = 5

INT_INVALID_ZERO = 0
INT_INVALID_NEGATIVE = -5


@pytest.fixture
def mock_s3_uri_service() -> MagicMock:
    return MagicMock()


@pytest.fixture
def service(mock_s3_uri_service) -> LambdaConfigValidatorServiceImplemented:
    return LambdaConfigValidatorServiceImplemented(
        s3_uri_service=mock_s3_uri_service
    )


@pytest.fixture
def mock_manifest() -> MagicMock:
    manifest = MagicMock(spec=InputManifest)
    manifest.workers = INT_VALID_WORKERS
    manifest.threads_per_worker = INT_VALID_THREADS
    manifest.global_rate_limit = INT_VALID_LIMIT
    manifest.window_seconds = INT_VALID_WINDOW
    manifest.calls_per_model = INT_VALID_CALLS
    manifest.source_csv_path = VAL_VALID_PATH
    return manifest


def test_validate_input_success(service, mock_s3_uri_service, mock_manifest):
    manifest_object = cast(InputManifest, cast(object, mock_manifest))

    assert service.validate_input(manifest_object) is None
    mock_s3_uri_service.parse_s3_uri.assert_called_once_with(VAL_VALID_PATH)


@pytest.mark.parametrize("invalid_worker", [INT_INVALID_ZERO, INT_INVALID_NEGATIVE])
def test_validate_input_invalid_workers_raises_value_error(
    service, mock_manifest, invalid_worker
):
    mock_manifest.workers = invalid_worker
    manifest_object = cast(InputManifest, cast(object, mock_manifest))

    with pytest.raises(ValueError) as exc_info:
        service.validate_input(manifest_object)

    assert str(exc_info.value) == ERR_WORKERS


@pytest.mark.parametrize("invalid_thread", [INT_INVALID_ZERO, INT_INVALID_NEGATIVE])
def test_validate_input_invalid_threads_raises_value_error(
    service, mock_manifest, invalid_thread
):
    mock_manifest.threads_per_worker = invalid_thread
    manifest_object = cast(InputManifest, cast(object, mock_manifest))

    with pytest.raises(ValueError) as exc_info:
        service.validate_input(manifest_object)

    assert str(exc_info.value) == ERR_THREADS


@pytest.mark.parametrize("invalid_limit", [INT_INVALID_ZERO, INT_INVALID_NEGATIVE])
def test_validate_input_invalid_global_rate_limit_raises_value_error(
    service, mock_manifest, invalid_limit
):
    mock_manifest.global_rate_limit = invalid_limit
    manifest_object = cast(InputManifest, cast(object, mock_manifest))

    with pytest.raises(ValueError) as exc_info:
        service.validate_input(manifest_object)

    assert str(exc_info.value) == ERR_RATE_LIMIT


@pytest.mark.parametrize("invalid_window", [INT_INVALID_ZERO, INT_INVALID_NEGATIVE])
def test_validate_input_invalid_window_seconds_raises_value_error(
    service, mock_manifest, invalid_window
):
    mock_manifest.window_seconds = invalid_window
    manifest_object = cast(InputManifest, cast(object, mock_manifest))

    with pytest.raises(ValueError) as exc_info:
        service.validate_input(manifest_object)

    assert str(exc_info.value) == ERR_WINDOW


@pytest.mark.parametrize("invalid_calls", [INT_INVALID_ZERO, INT_INVALID_NEGATIVE])
def test_validate_input_invalid_calls_per_model_raises_value_error(
    service, mock_manifest, invalid_calls
):
    mock_manifest.calls_per_model = invalid_calls
    manifest_object = cast(InputManifest, cast(object, mock_manifest))

    with pytest.raises(ValueError) as exc_info:
        service.validate_input(manifest_object)

    assert str(exc_info.value) == ERR_CALLS


def test_validate_input_missing_source_csv_path_raises_value_error(
    service, mock_manifest
):
    mock_manifest.source_csv_path = VAL_EMPTY_PATH
    manifest_object = cast(InputManifest, cast(object, mock_manifest))

    with pytest.raises(ValueError) as exc_info:
        service.validate_input(manifest_object)

    assert str(exc_info.value) == ERR_PATH_REQUIRED