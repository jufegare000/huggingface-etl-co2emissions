from typing import cast
from unittest.mock import MagicMock
import pytest

from domain.data_preparation.models.preparation.boundary import Boundary
from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.model_metadata import ModelMetadata
from domain.data_preparation.models.preparation.partition_status import PartitionStatus
from domain.data_preparation.services.s3.s3_service import S3Service
from application.data_preparation.services.config.partitions.partition_descriptor_service_implemented import (
    PartitionDescriptorServiceImplemented,
)

VAL_BUCKET = "analytics-raw-zone"
VAL_PREFIX_RAW = "/gold/prepared_transcripts/"
VAL_PREFIX_STRIPPED = "gold/prepared_transcripts"

VAL_SLASH = "/"
VAL_S3_SCHEMA = "s3://"
VAL_PARTITION_SEGMENT = "/partition_id="
VAL_CSV_SUFFIX = "/ai_metadata_models.csv"

INT_THREADS = 8

INT_ID_1 = 1
VAL_ID_STR_1 = "000001"
INT_START_1 = 0
INT_END_1 = 2
INT_RECORDS_1 = 150
FLT_MIN_1 = 10.5
FLT_MAX_1 = 45.2

INT_ID_2 = 104
VAL_ID_STR_2 = "000104"
INT_START_2 = 2
INT_END_2 = 3
INT_RECORDS_2 = 75
FLT_MIN_2 = 45.3
FLT_MAX_2 = 90.0

INT_ZERO = 0
INT_ONE = 1
INT_TWO = 2


@pytest.fixture
def mock_s3_service() -> MagicMock:
    return MagicMock(spec=S3Service)


@pytest.fixture
def service(mock_s3_service) -> PartitionDescriptorServiceImplemented:
    return PartitionDescriptorServiceImplemented(
        s3_service=cast(S3Service, cast(object, mock_s3_service))
    )


@pytest.fixture
def mock_config() -> MagicMock:
    config = MagicMock(spec=InputManifest)
    config.bucket_name = VAL_BUCKET
    config.prepared_prefix = VAL_PREFIX_RAW
    config.threads_per_worker = INT_THREADS
    return config


@pytest.fixture
def mock_boundaries() -> list[MagicMock]:
    boundary_1 = MagicMock(spec=Boundary)
    boundary_1.partition_id = INT_ID_1
    boundary_1.start_index = INT_START_1
    boundary_1.end_index = INT_END_1
    boundary_1.records_count = INT_RECORDS_1
    boundary_1.emission_min = FLT_MIN_1
    boundary_1.emission_max = FLT_MAX_1

    boundary_2 = MagicMock(spec=Boundary)
    boundary_2.partition_id = INT_ID_2
    boundary_2.start_index = INT_START_2
    boundary_2.end_index = INT_END_2
    boundary_2.records_count = INT_RECORDS_2
    boundary_2.emission_min = FLT_MIN_2
    boundary_2.emission_max = FLT_MAX_2

    return [boundary_1, boundary_2]


@pytest.fixture
def mock_models() -> list[MagicMock]:
    model_a = MagicMock(spec=ModelMetadata)
    model_b = MagicMock(spec=ModelMetadata)
    model_c = MagicMock(spec=ModelMetadata)
    return [model_a, model_b, model_c]


def test_build_partition_descriptors_success(
    service, mock_s3_service, mock_config, mock_boundaries, mock_models
):
    boundaries_list = cast(list[Boundary], cast(object, mock_boundaries))
    config_object = cast(InputManifest, cast(object, mock_config))
    models_list = cast(list[ModelMetadata], cast(object, mock_models))

    expected_key_1 = (
        VAL_PREFIX_STRIPPED + VAL_PARTITION_SEGMENT + VAL_ID_STR_1 + VAL_CSV_SUFFIX
    )
    expected_path_1 = (
        VAL_S3_SCHEMA + VAL_BUCKET + VAL_SLASH + expected_key_1
    )

    expected_key_2 = (
        VAL_PREFIX_STRIPPED + VAL_PARTITION_SEGMENT + VAL_ID_STR_2 + VAL_CSV_SUFFIX
    )
    expected_path_2 = (
        VAL_S3_SCHEMA + VAL_BUCKET + VAL_SLASH + expected_key_2
    )

    expected_rows_1 = mock_models[INT_START_1:INT_END_1]
    expected_rows_2 = mock_models[INT_START_2:INT_END_2]

    result = service.build_partition_descriptors(
        boundaries=boundaries_list, config=config_object, models=models_list
    )

    assert len(result) == INT_TWO

    assert result[INT_ZERO].partition_id == VAL_ID_STR_1
    assert result[INT_ZERO].input_path == expected_path_1
    assert result[INT_ZERO].thread_count == INT_THREADS
    assert result[INT_ZERO].records_count == INT_RECORDS_1
    assert result[INT_ZERO].emission_min == FLT_MIN_1
    assert result[INT_ZERO].emission_max == FLT_MAX_1
    assert result[INT_ZERO].status == PartitionStatus.PENDING

    assert result[INT_ONE].partition_id == VAL_ID_STR_2
    assert result[INT_ONE].input_path == expected_path_2
    assert result[INT_ONE].thread_count == INT_THREADS
    assert result[INT_ONE].records_count == INT_RECORDS_2
    assert result[INT_ONE].emission_min == FLT_MIN_2
    assert result[INT_ONE].emission_max == FLT_MAX_2
    assert result[INT_ONE].status == PartitionStatus.PENDING

    assert mock_s3_service.write_csv_to_s3.call_count == INT_TWO
    mock_s3_service.write_csv_to_s3.assert_any_call(
        rows=expected_rows_1, bucket=VAL_BUCKET, key=expected_key_1
    )
    mock_s3_service.write_csv_to_s3.assert_any_call(
        rows=expected_rows_2, bucket=VAL_BUCKET, key=expected_key_2
    )


def test_build_partition_descriptors_empty_boundaries(
    service, mock_s3_service, mock_config, mock_models
):
    empty_boundaries = cast(list[Boundary], cast(object, []))
    config_object = cast(InputManifest, cast(object, mock_config))
    models_list = cast(list[ModelMetadata], cast(object, mock_models))

    result = service.build_partition_descriptors(
        boundaries=empty_boundaries, config=config_object, models=models_list
    )

    assert len(result) == INT_ZERO
    mock_s3_service.write_csv_to_s3.assert_not_called()