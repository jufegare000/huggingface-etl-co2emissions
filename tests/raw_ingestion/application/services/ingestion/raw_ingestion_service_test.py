from unittest.mock import MagicMock, call

import pytest

from raw_ingestion.application.services.enrichment.row_enrichment_service import RowEnrichmentService
from raw_ingestion.application.services.ingestion.raw_ingestion_service import RawIngestionService
from raw_ingestion.application.services.rate_limit.rate_limit_service import RateLimitService
from raw_ingestion.domain.models.exceptions import PartitionAlreadyCompletedException
from raw_ingestion.domain.models.ingestion_metrics import IngestionMetrics
from raw_ingestion.domain.models.partition_config import PartitionConfig

RUN_ID = "run-001"
PARTITION_ID = "000001"
JOB_NAME = "test-job"
HF_TOKEN = "hf_fake"
NOW = "2024-01-01T12:00:00+00:00"
BUCKET = "my-bucket"
INPUT_PATH = f"s3://{BUCKET}/input/partition.csv"

CSV_ROWS = [{"model_id": "org/model-a"}, {"model_id": "org/model-b"}]
HF_PAYLOAD = {"id": "org/model-a", "downloads": 100, "likes": 5}
ENRICHED_ROW = {"modelId": "org/model-a", "run_id": RUN_ID}
ERROR_ROW = {"model_id": "org/model-x", "error_type": "RuntimeError"}

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
    m.read_partition_csv.return_value = CSV_ROWS
    return m


@pytest.fixture
def mock_enrichment():
    m = MagicMock(spec=RowEnrichmentService)
    m.build_enriched_row.return_value = ENRICHED_ROW
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
    return RawIngestionService(
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
    metrics = service.run(RUN_ID, PARTITION_ID, JOB_NAME)
    assert isinstance(metrics, IngestionMetrics)


def test_run_fetches_hf_token(service, mock_secret_obtainer):
    service.run(RUN_ID, PARTITION_ID, JOB_NAME)
    mock_secret_obtainer.get_secret_token.assert_called_once_with()


def test_run_marks_partition_running(service, mock_partition_repo):
    service.run(RUN_ID, PARTITION_ID, JOB_NAME)
    mock_partition_repo.mark_running.assert_called_once_with(RUN_ID, PARTITION_ID, JOB_NAME)


def test_run_marks_partition_completed_on_success(service, mock_partition_repo):
    service.run(RUN_ID, PARTITION_ID, JOB_NAME)
    mock_partition_repo.mark_completed.assert_called_once()
    call_kwargs = mock_partition_repo.mark_completed.call_args.kwargs
    assert call_kwargs["run_id"] == RUN_ID
    assert call_kwargs["partition_id"] == PARTITION_ID


def test_run_counts_success_rows(service):
    metrics = service.run(RUN_ID, PARTITION_ID, JOB_NAME)
    assert metrics.success_count == 2


def test_run_counts_api_calls(service):
    metrics = service.run(RUN_ID, PARTITION_ID, JOB_NAME)
    assert metrics.api_calls_count == 2


def test_run_writes_success_marker(service, mock_writer):
    service.run(RUN_ID, PARTITION_ID, JOB_NAME)
    mock_writer.write_success_marker.assert_called_once()
    args = mock_writer.write_success_marker.call_args
    payload = args.args[0]
    assert payload["status"] == "COMPLETED"


def test_run_writes_metrics(service, mock_writer):
    service.run(RUN_ID, PARTITION_ID, JOB_NAME)
    mock_writer.write_metrics.assert_called_once()


# --- failure handling ---

def test_run_marks_partition_failed_on_exception(service, mock_partition_repo):
    mock_partition_repo.get_config.side_effect = RuntimeError("dynamo down")

    with pytest.raises(RuntimeError):
        service.run(RUN_ID, PARTITION_ID, JOB_NAME)

    mock_partition_repo.mark_failed.assert_called_once()


def test_run_reraises_exception(service, mock_partition_repo):
    mock_partition_repo.mark_running.side_effect = RuntimeError("hard fail")

    with pytest.raises(RuntimeError, match="hard fail"):
        service.run(RUN_ID, PARTITION_ID, JOB_NAME)


def test_run_skips_when_partition_already_completed(service, mock_partition_repo, mock_writer):
    mock_partition_repo.mark_running.side_effect = PartitionAlreadyCompletedException(
        "already done"
    )

    metrics = service.run(RUN_ID, PARTITION_ID, JOB_NAME)

    assert isinstance(metrics, IngestionMetrics)
    mock_partition_repo.mark_completed.assert_not_called()
    mock_partition_repo.mark_failed.assert_not_called()
    mock_writer.write_batch.assert_not_called()
    mock_writer.write_success_marker.assert_not_called()


# --- missing model_id ---

def test_run_counts_missing_model_id_as_failed(service, mock_writer, mock_enrichment):
    mock_writer.read_partition_csv.return_value = [{"model_id": ""}, {"model_id": "org/ok"}]
    mock_enrichment.build_enriched_row.return_value = ENRICHED_ROW

    metrics = service.run(RUN_ID, PARTITION_ID, JOB_NAME)

    assert metrics.failed_count == 1
    assert metrics.success_count == 1


# --- batch flushing ---

def test_run_flushes_batch_when_batch_size_reached(service, mock_writer, mock_enrichment):
    rows = [{"model_id": f"org/model-{i}"} for i in range(50)]
    mock_writer.read_partition_csv.return_value = rows
    mock_enrichment.build_enriched_row.return_value = ENRICHED_ROW

    service.run(RUN_ID, PARTITION_ID, JOB_NAME)

    mock_writer.write_batch.assert_called()
    assert mock_writer.write_batch.call_count >= 1


def test_run_writes_errors_when_api_call_fails_for_a_row(
    service, mock_writer, mock_hf_fetcher, mock_enrichment
):
    mock_hf_fetcher.fetch_model.side_effect = RuntimeError("model not found")
    mock_enrichment.build_error_row.return_value = ERROR_ROW

    metrics = service.run(RUN_ID, PARTITION_ID, JOB_NAME)

    assert metrics.failed_count == 2
    mock_writer.write_errors.assert_called_once()
