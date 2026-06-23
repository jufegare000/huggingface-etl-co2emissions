from typing import cast
from unittest.mock import MagicMock
import pytest

from domain.data_preparation.models.preparation.final_manifest import FinalManifest
from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor
from domain.data_preparation.services.date_parsing_service import DataParsingService
from infrastructure.out.dynamo.mappers.impl.dynamo_db_data_preparation_mapper_implemented import \
    DynamoDBDataPreparationMapperImplemented

VAL_NOW = "2026-06-22T12:00:00Z"

PK_PIPELINE = "PIPELINE#hf-carbon"
SK_RUN_EXPECTED = "RUN#run-123"
SK_LAST_RUN = "LAST_RUN"
SK_PARTITION_EXPECTED = "PARTITION#part-456"
PK_PARTITION_EXPECTED = "RUN#run-123"

ENTITY_RUN = "PREPARATION_RUN"
ENTITY_LAST_RUN = "LAST_RUN_POINTER"
ENTITY_PARTITION = "PARTITION"

STATUS_PREPARED = "PREPARED"
STATUS_PENDING = "PENDING"

VAL_RUN_ID = "run-123"
VAL_SOURCE_CSV = "s3://bucket/source.csv"
VAL_MANIFEST_PATH = "s3://bucket/manifest.json"
VAL_PARTITION_ID = "part-456"
VAL_INPUT_PATH = "s3://bucket/part-456.csv"

INT_WORKERS = 4
INT_THREADS_PER_WORKER = 8
INT_RATE_LIMIT = 100
INT_WINDOW_SECONDS = 60
INT_CALLS_PER_MODEL = 2
INT_RECORDS_COUNT = 500
INT_THREAD_COUNT = 4

FLT_EMISSION_MIN = 0.15
FLT_EMISSION_MAX = 9.81

INT_ZERO = 0
INT_PARTITIONS_COUNT = 2
INT_EXPECTED_BUDGET = 1000
VAL_NONE = None

VAL_EXPECTED_OUTPUT_PREFIX = "enriched/hf-carbon/run_id=run-123/partition_id=part-456/"

KEY_PK = "PK"
KEY_SK = "SK"
KEY_ENTITY_TYPE = "entity_type"
KEY_RUN_ID = "run_id"
KEY_STATUS = "status"
KEY_SOURCE_CSV_PATH = "source_csv_path"
KEY_MANIFEST_PATH = "manifest_path"
KEY_PARTITIONS_COUNT = "partitions_count"
KEY_WORKERS = "workers"
KEY_THREADS_PER_WORKER = "threads_per_worker"
KEY_GLOBAL_RATE_LIMIT = "global_rate_limit"
KEY_WINDOW_SECONDS = "window_seconds"
KEY_CALLS_PER_MODEL = "calls_per_model"
KEY_CREATED_AT = "created_at"
KEY_UPDATED_AT = "updated_at"
KEY_PARTITION_ID = "partition_id"
KEY_INPUT_PATH = "input_path"
KEY_RECORDS_COUNT = "records_count"
KEY_THREAD_COUNT = "thread_count"
KEY_EMISSION_MIN = "emission_min"
KEY_EMISSION_MAX = "emission_max"
KEY_INPUT = "input"
KEY_OUTPUT = "output"
KEY_RATE_BUDGET = "rate_budget"
KEY_EXECUTION = "execution"
KEY_METRICS = "metrics"
KEY_ERROR = "error"

KEY_SUB_URI = "uri"
KEY_SUB_PREFIX = "prefix"
KEY_SUB_RESULTS_PATH = "results_path"
KEY_SUB_ERRORS_PATH = "errors_path"
KEY_SUB_METRICS_PATH = "metrics_path"
KEY_SUB_SUCCESS_MARKER = "success_marker_path"
KEY_SUB_PART_BUDGET = "partition_call_budget"
KEY_SUB_EST_CALLS = "estimated_calls"
KEY_SUB_ATTEMPTS = "attempts"
KEY_SUB_STARTED_AT = "started_at"
KEY_SUB_COMPLETED_AT = "completed_at"
KEY_SUB_GLUE_NAME = "glue_job_name"
KEY_SUB_GLUE_RUN_ID = "glue_job_run_id"
KEY_SUB_INPUT_COUNT = "input_count"
KEY_SUB_PROCESSED_COUNT = "processed_count"
KEY_SUB_SUCCESS_COUNT = "success_count"
KEY_SUB_FAILED_COUNT = "failed_count"
KEY_SUB_SKIPPED_COUNT = "skipped_count"
KEY_SUB_API_CALLS = "api_calls_count"
KEY_SUB_BATCHES_WRITTEN = "batches_written"
KEY_SUB_LAST_BATCH = "last_batch_path"
KEY_SUB_ERR_TYPE = "last_error_type"
KEY_SUB_ERR_MSG = "last_error_message"


@pytest.fixture
def mock_data_parsing_service() -> MagicMock:
    service = MagicMock(spec=DataParsingService)
    service.utc_now_iso.return_value = VAL_NOW
    return service


@pytest.fixture
def mapper(mock_data_parsing_service) -> DynamoDBDataPreparationMapperImplemented:
    return DynamoDBDataPreparationMapperImplemented(
        data_parsing_service=cast(DataParsingService, cast(object, mock_data_parsing_service))
    )


@pytest.fixture
def mock_config() -> MagicMock:
    config = MagicMock(spec=InputManifest)
    config.run_id = VAL_RUN_ID
    config.source_csv_path = VAL_SOURCE_CSV
    config.workers = INT_WORKERS
    config.threads_per_worker = INT_THREADS_PER_WORKER
    config.global_rate_limit = INT_RATE_LIMIT
    config.window_seconds = INT_WINDOW_SECONDS
    config.calls_per_model = INT_CALLS_PER_MODEL
    return config


@pytest.fixture
def mock_manifest() -> MagicMock:
    manifest = MagicMock(spec=FinalManifest)
    manifest.manifest_path = VAL_MANIFEST_PATH
    manifest.partitions = [MagicMock(), MagicMock()]
    return manifest


@pytest.fixture
def mock_partition() -> MagicMock:
    partition = MagicMock(spec=PartitionDescriptor)
    partition.partition_id = VAL_PARTITION_ID
    partition.input_path = VAL_INPUT_PATH
    partition.records_count = INT_RECORDS_COUNT
    partition.thread_count = INT_THREAD_COUNT
    partition.emission_min = FLT_EMISSION_MIN
    partition.emission_max = FLT_EMISSION_MAX
    return partition


def test_to_run_item(mapper, mock_config, mock_manifest, mock_data_parsing_service):
    result = mapper.to_run_item(mock_config, mock_manifest)

    assert result[KEY_PK] == PK_PIPELINE
    assert result[KEY_SK] == SK_RUN_EXPECTED
    assert result[KEY_ENTITY_TYPE] == ENTITY_RUN
    assert result[KEY_RUN_ID] == VAL_RUN_ID
    assert result[KEY_STATUS] == STATUS_PREPARED
    assert result[KEY_SOURCE_CSV_PATH] == VAL_SOURCE_CSV
    assert result[KEY_MANIFEST_PATH] == VAL_MANIFEST_PATH
    assert result[KEY_PARTITIONS_COUNT] == INT_PARTITIONS_COUNT
    assert result[KEY_WORKERS] == INT_WORKERS
    assert result[KEY_THREADS_PER_WORKER] == INT_THREADS_PER_WORKER
    assert result[KEY_GLOBAL_RATE_LIMIT] == INT_RATE_LIMIT
    assert result[KEY_WINDOW_SECONDS] == INT_WINDOW_SECONDS
    assert result[KEY_CALLS_PER_MODEL] == INT_CALLS_PER_MODEL
    assert result[KEY_CREATED_AT] == VAL_NOW
    assert result[KEY_UPDATED_AT] == VAL_NOW
    mock_data_parsing_service.utc_now_iso.assert_called_once()


def test_to_last_run_item(mapper, mock_config, mock_manifest, mock_data_parsing_service):
    result = mapper.to_last_run_item(mock_config, mock_manifest)

    assert result[KEY_PK] == PK_PIPELINE
    assert result[KEY_SK] == SK_LAST_RUN
    assert result[KEY_ENTITY_TYPE] == ENTITY_LAST_RUN
    assert result[KEY_RUN_ID] == VAL_RUN_ID
    assert result[KEY_STATUS] == STATUS_PREPARED
    assert result[KEY_SOURCE_CSV_PATH] == VAL_SOURCE_CSV
    assert result[KEY_MANIFEST_PATH] == VAL_MANIFEST_PATH
    assert result[KEY_PARTITIONS_COUNT] == INT_PARTITIONS_COUNT
    assert result[KEY_UPDATED_AT] == VAL_NOW
    mock_data_parsing_service.utc_now_iso.assert_called_once()


def test_to_partition_item(mapper, mock_config, mock_partition, mock_data_parsing_service):
    result = mapper.to_partition_item(mock_config, mock_partition)

    assert result[KEY_PK] == PK_PARTITION_EXPECTED
    assert result[KEY_SK] == SK_PARTITION_EXPECTED
    assert result[KEY_ENTITY_TYPE] == ENTITY_PARTITION
    assert result[KEY_RUN_ID] == VAL_RUN_ID
    assert result[KEY_PARTITION_ID] == VAL_PARTITION_ID
    assert result[KEY_STATUS] == STATUS_PENDING
    assert result[KEY_INPUT_PATH] == VAL_INPUT_PATH
    assert result[KEY_RECORDS_COUNT] == INT_RECORDS_COUNT
    assert result[KEY_THREAD_COUNT] == INT_THREAD_COUNT
    assert result[KEY_EMISSION_MIN] == FLT_EMISSION_MIN
    assert result[KEY_EMISSION_MAX] == FLT_EMISSION_MAX

    assert result[KEY_INPUT][KEY_SUB_URI] == VAL_INPUT_PATH
    assert result[KEY_INPUT][KEY_RECORDS_COUNT] == INT_RECORDS_COUNT

    assert result[KEY_OUTPUT][KEY_SUB_PREFIX] == VAL_EXPECTED_OUTPUT_PREFIX
    assert result[KEY_OUTPUT][KEY_SUB_RESULTS_PATH] == VAL_NONE
    assert result[KEY_OUTPUT][KEY_SUB_ERRORS_PATH] == VAL_NONE
    assert result[KEY_OUTPUT][KEY_SUB_METRICS_PATH] == VAL_NONE
    assert result[KEY_OUTPUT][KEY_SUB_SUCCESS_MARKER] == VAL_NONE

    assert result[KEY_RATE_BUDGET][KEY_SUB_PART_BUDGET] == INT_EXPECTED_BUDGET
    assert result[KEY_RATE_BUDGET][KEY_SUB_EST_CALLS] == INT_EXPECTED_BUDGET
    assert result[KEY_RATE_BUDGET][KEY_GLOBAL_RATE_LIMIT] == INT_RATE_LIMIT
    assert result[KEY_RATE_BUDGET][KEY_WINDOW_SECONDS] == INT_WINDOW_SECONDS
    assert result[KEY_RATE_BUDGET][KEY_CALLS_PER_MODEL] == INT_CALLS_PER_MODEL

    assert result[KEY_EXECUTION][KEY_SUB_ATTEMPTS] == INT_ZERO
    assert result[KEY_EXECUTION][KEY_SUB_STARTED_AT] == VAL_NONE
    assert result[KEY_EXECUTION][KEY_SUB_COMPLETED_AT] == VAL_NONE
    assert result[KEY_EXECUTION][KEY_SUB_GLUE_NAME] == VAL_NONE
    assert result[KEY_EXECUTION][KEY_SUB_GLUE_RUN_ID] == VAL_NONE

    assert result[KEY_METRICS][KEY_SUB_INPUT_COUNT] == INT_RECORDS_COUNT
    assert result[KEY_METRICS][KEY_SUB_PROCESSED_COUNT] == INT_ZERO
    assert result[KEY_METRICS][KEY_SUB_SUCCESS_COUNT] == INT_ZERO
    assert result[KEY_METRICS][KEY_SUB_FAILED_COUNT] == INT_ZERO
    assert result[KEY_METRICS][KEY_SUB_SKIPPED_COUNT] == INT_ZERO
    assert result[KEY_METRICS][KEY_SUB_API_CALLS] == INT_ZERO
    assert result[KEY_METRICS][KEY_SUB_BATCHES_WRITTEN] == INT_ZERO
    assert result[KEY_METRICS][KEY_SUB_LAST_BATCH] == VAL_NONE

    assert result[KEY_ERROR][KEY_SUB_ERR_TYPE] == VAL_NONE
    assert result[KEY_ERROR][KEY_SUB_ERR_MSG] == VAL_NONE

    assert result[KEY_CREATED_AT] == VAL_NOW
    assert result[KEY_UPDATED_AT] == VAL_NOW
    mock_data_parsing_service.utc_now_iso.assert_called_once()