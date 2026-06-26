from unittest.mock import MagicMock, call

import pytest

from raw_ingestion.application.services.enrichment.row_enrichment_service import RowEnrichmentService
from raw_ingestion.application.services.rate_limit.rate_limit_service import RateLimitService
from raw_ingestion.application.services.recuperation.data_recuperation_service import (
    DataRecuperationService,
)
from raw_ingestion.domain.models.partition_config import PartitionConfig
from raw_ingestion.domain.models.recuperation_metrics import RecuperationMetrics

RUN_ID = "run-001"
BUCKET = "my-bucket"
PARTITION_ID = "000001"
HF_TOKEN = "hf_fake"
NOW = "2024-01-01T12:00:00+00:00"
INPUT_PATH = f"s3://{BUCKET}/input/partition.csv"

ERROR_429_ROW = {
    "run_id": RUN_ID,
    "partition_id": PARTITION_ID,
    "model_id": "org/rate-limited-model",
    "error_type": "RuntimeError",
    "error_message": "HF API HTTP 429 for model_id=org/rate-limited-model: rate limit exceeded",
    "failed_at": NOW,
}
ERROR_404_ROW = {
    "run_id": RUN_ID,
    "partition_id": PARTITION_ID,
    "model_id": "org/not-found-model",
    "error_type": "RuntimeError",
    "error_message": "HF API HTTP 404 for model_id=org/not-found-model: not found",
    "failed_at": NOW,
}
CSV_ROWS = [
    {"model_id": "org/rate-limited-model", "co2_eq_emissions": "1.5"},
    {"model_id": "org/other-model", "co2_eq_emissions": "2.0"},
]
HF_PAYLOAD = {"id": "org/rate-limited-model", "downloads": 500, "likes": 10}
ENRICHED_ROW = {"model_id": "org/rate-limited-model", "run_id": RUN_ID}
ERROR_ROW = {"model_id": "org/rate-limited-model", "error_type": "RuntimeError"}


def _make_partition_config(**kwargs) -> PartitionConfig:
    defaults = dict(
        run_id=RUN_ID,
        partition_id=PARTITION_ID,
        input_path=INPUT_PATH,
        records_count=2,
        rate_budget=None,
    )
    return PartitionConfig(**{**defaults, **kwargs})


@pytest.fixture
def mock_hf_fetcher():
    m = MagicMock()
    m.fetch_model.return_value = HF_PAYLOAD
    return m


@pytest.fixture
def mock_partition_repo():
    m = MagicMock()
    m.get_config.return_value = _make_partition_config()
    return m


@pytest.fixture
def mock_writer():
    m = MagicMock()
    m.list_error_files.return_value = [
        f"enriched/hf-carbon/run_id={RUN_ID}/partition_id={PARTITION_ID}/errors/errors.jsonl"
    ]
    m.read_jsonl_lines.return_value = [ERROR_429_ROW]
    m.read_partition_csv.return_value = CSV_ROWS
    return m


@pytest.fixture
def mock_enrichment():
    m = MagicMock(spec=RowEnrichmentService)
    m.build_enriched_row.return_value = ENRICHED_ROW
    m.build_error_row.return_value = ERROR_ROW
    return m


@pytest.fixture
def mock_rate_limit():
    m = MagicMock(spec=RateLimitService)
    m.sleep_if_budget_reached.return_value = (0, 0.0)
    return m


@pytest.fixture
def mock_date_service():
    m = MagicMock()
    m.utc_now_iso.return_value = NOW
    return m


@pytest.fixture
def mock_secret_obtainer():
    m = MagicMock()
    m.get_secret_token.return_value = HF_TOKEN
    return m


@pytest.fixture
def service(
    mock_hf_fetcher,
    mock_partition_repo,
    mock_writer,
    mock_enrichment,
    mock_rate_limit,
    mock_date_service,
    mock_secret_obtainer,
):
    return DataRecuperationService(
        hf_model_fetcher=mock_hf_fetcher,
        partition_repository=mock_partition_repo,
        ingestion_writer=mock_writer,
        row_enrichment_service=mock_enrichment,
        rate_limit_service=mock_rate_limit,
        date_parsing_service=mock_date_service,
        secret_obtainer=mock_secret_obtainer,
    )


# --- happy path ---

def test_run_returns_metrics(service):
    metrics = service.run(RUN_ID, BUCKET)
    assert isinstance(metrics, RecuperationMetrics)


def test_run_fetches_hf_token(service, mock_secret_obtainer):
    service.run(RUN_ID, BUCKET)
    mock_secret_obtainer.get_secret_token.assert_called_once()


def test_run_lists_error_files_for_run(service, mock_writer):
    service.run(RUN_ID, BUCKET)
    mock_writer.list_error_files.assert_called_once_with(BUCKET, RUN_ID)


def test_run_counts_rate_limit_errors(service):
    metrics = service.run(RUN_ID, BUCKET)
    assert metrics.rate_limit_errors_found == 1


def test_run_ignores_non_429_errors(service, mock_writer):
    mock_writer.read_jsonl_lines.return_value = [ERROR_404_ROW]
    metrics = service.run(RUN_ID, BUCKET)
    assert metrics.rate_limit_errors_found == 0
    assert metrics.recuperated_count == 0


def test_run_fetches_model_for_each_rate_limited_entry(service, mock_hf_fetcher):
    service.run(RUN_ID, BUCKET)
    mock_hf_fetcher.fetch_model.assert_called_once_with("org/rate-limited-model", HF_TOKEN)


def test_run_reads_original_csv_to_recover_source_fields(service, mock_writer, mock_partition_repo):
    service.run(RUN_ID, BUCKET)
    mock_partition_repo.get_config.assert_called_once_with(RUN_ID, PARTITION_ID)
    mock_writer.read_partition_csv.assert_called_once_with(BUCKET, "input/partition.csv")


def test_run_writes_recuperated_batch_to_batches_prefix(service, mock_writer):
    service.run(RUN_ID, BUCKET)
    mock_writer.write_batch.assert_called_once()
    _, args, _ = mock_writer.write_batch.mock_calls[0]
    batch_key = args[2]
    assert f"run_id={RUN_ID}" in batch_key
    assert f"partition_id={PARTITION_ID}" in batch_key
    assert "/batches/recuperation_" in batch_key
    assert batch_key.endswith(".jsonl")


def test_run_counts_recuperated(service):
    metrics = service.run(RUN_ID, BUCKET)
    assert metrics.recuperated_count == 1


def test_run_writes_summary(service, mock_writer):
    service.run(RUN_ID, BUCKET)
    mock_writer.write_metrics.assert_called_once()
    payload = mock_writer.write_metrics.call_args.args[0]
    assert payload["run_id"] == RUN_ID
    assert "recuperated_count" in payload


def test_run_no_error_files_returns_zero_metrics(service, mock_writer):
    mock_writer.list_error_files.return_value = []
    metrics = service.run(RUN_ID, BUCKET)
    assert metrics.rate_limit_errors_found == 0
    assert metrics.recuperated_count == 0
    mock_writer.write_batch.assert_not_called()


# --- still-failing models ---

def test_run_records_still_failed_when_refetch_raises(service, mock_hf_fetcher, mock_writer):
    mock_hf_fetcher.fetch_model.side_effect = RuntimeError("HF API HTTP 429 again")
    metrics = service.run(RUN_ID, BUCKET)
    assert metrics.still_failed_count == 1
    assert metrics.recuperated_count == 0
    mock_writer.write_errors.assert_called_once()


def test_run_writes_still_failed_to_recuperation_prefix(service, mock_hf_fetcher, mock_writer):
    mock_hf_fetcher.fetch_model.side_effect = RuntimeError("still rate limited")
    service.run(RUN_ID, BUCKET)
    error_key = mock_writer.write_errors.call_args.args[2]
    assert f"run_id={RUN_ID}/recuperation/" in error_key


def test_run_does_not_write_still_failed_file_when_all_succeed(service, mock_writer):
    service.run(RUN_ID, BUCKET)
    mock_writer.write_errors.assert_not_called()


# --- rate limiting ---

def test_run_calls_rate_limiter_before_each_fetch(service, mock_rate_limit):
    service.run(RUN_ID, BUCKET)
    mock_rate_limit.sleep_if_budget_reached.assert_called_once()


# --- mixed error files ---

def test_run_filters_only_429_from_mixed_errors(service, mock_writer):
    mock_writer.read_jsonl_lines.return_value = [ERROR_429_ROW, ERROR_404_ROW]
    metrics = service.run(RUN_ID, BUCKET)
    assert metrics.rate_limit_errors_found == 1
    assert metrics.errors_scanned == 2
