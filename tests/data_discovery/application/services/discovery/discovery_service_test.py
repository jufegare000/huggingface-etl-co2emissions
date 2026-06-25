from unittest.mock import MagicMock, call

import pytest

from data_discovery.application.services.discovery.discovery_service import DiscoveryJobService
from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from data_discovery.domain.models.discovery_run_state import DiscoveryRunState
from shared.domain.exceptions.rate_limit_error import RateLimitError

SNAPSHOT_ID = "20240101T120000"
STARTED_AT = "2024-01-01T12:00:00Z"
NOW_ISO = "2024-01-01T14:00:00Z"
HF_TOKEN = "hf_fake"
TARGET_BUCKET = "my-bucket"
CURSOR = "cursor_page2"
SNAPSHOT_PATH = f"s3://{TARGET_BUCKET}/{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={SNAPSHOT_ID}/filtered/models_with_emissions.csv"
LATEST_PATH = f"s3://{TARGET_BUCKET}/{DiscoveryJobConstantsEnum.BASE_PREFIX}/latest/models_with_emissions.csv"

MODEL_ROW = {"model_id": "org/model-a", "co2_eq_emissions": "1.0"}


def _make_checkpoint():
    return {
        "snapshot_id": SNAPSHOT_ID,
        "status": "RUNNING",
        "next_cursor": None,
        "last_successful_page": 0,
        "total_models_seen": 0,
        "models_with_emissions": 0,
        "part_number": 0,
        "started_at": STARTED_AT,
        "updated_at": STARTED_AT,
    }


@pytest.fixture
def mock_date_service():
    m = MagicMock()
    m.utc_now_iso.return_value = NOW_ISO
    return m


@pytest.fixture
def mock_secrets():
    m = MagicMock()
    m.get_secret_token.return_value = HF_TOKEN
    return m


@pytest.fixture
def mock_hf_types():
    m = MagicMock()
    m.filter_page.return_value = ([MODEL_ROW], 1)
    return m


@pytest.fixture
def mock_hf_client():
    m = MagicMock()
    m.fetch_models_page.return_value = (
        [{"modelId": "org/model-a"}],
        None,
        {"RateLimit": "100", "RateLimit-Policy": "100;w=3600"},
    )
    return m


@pytest.fixture
def mock_checkpoint_service():
    m = MagicMock()
    m.load_or_create_checkpoint.return_value = _make_checkpoint()
    return m


@pytest.fixture
def mock_consolidation():
    m = MagicMock()
    m.consolidate_parts.return_value = (SNAPSHOT_PATH, 1)
    return m


@pytest.fixture
def mock_part_flusher():
    return MagicMock()


@pytest.fixture
def mock_rate_limit_handler():
    return MagicMock()


@pytest.fixture
def mock_event_logger():
    return MagicMock()


@pytest.fixture
def service(
    mock_date_service, mock_secrets, mock_hf_types, mock_hf_client,
    mock_checkpoint_service, mock_consolidation, mock_part_flusher,
    mock_rate_limit_handler, mock_event_logger,
):
    return DiscoveryJobService(
        date_parsing_service=mock_date_service,
        secrets_obtainer=mock_secrets,
        hugging_face_types_helpers=mock_hf_types,
        hf_api_client=mock_hf_client,
        checkpoint_service=mock_checkpoint_service,
        consolidation_service=mock_consolidation,
        part_flusher=mock_part_flusher,
        rate_limit_handler=mock_rate_limit_handler,
        event_logger=mock_event_logger,
        target_bucket=TARGET_BUCKET,
    )


# --- happy path ---

def test_run_discovery_returns_completed_result(service):
    result = service.run_discovery()
    assert result["status"] == "COMPLETED"
    assert result["snapshot_id"] == SNAPSHOT_ID


def test_run_discovery_calls_consolidation(service, mock_consolidation):
    service.run_discovery()
    mock_consolidation.consolidate_parts.assert_called_once_with(SNAPSHOT_ID)


def test_run_discovery_saves_completed_checkpoint(service, mock_checkpoint_service):
    service.run_discovery()
    saved_checkpoints = [
        c.args[0] for c in mock_checkpoint_service.save_checkpoint.call_args_list
    ]
    final = saved_checkpoints[-1]
    assert final["status"] == "COMPLETED"
    assert final["next_cursor"] is None


def test_run_discovery_saves_result_progress_event(service, mock_checkpoint_service):
    service.run_discovery()
    mock_checkpoint_service.save_progress_event.assert_called()
    event_names = [c.args[1] for c in mock_checkpoint_service.save_progress_event.call_args_list]
    assert "result" in event_names


def test_run_discovery_logs_started_and_completed(service, mock_event_logger):
    service.run_discovery()
    mock_event_logger.log_started.assert_called_once()
    mock_event_logger.log_completed.assert_called_once()


def test_run_discovery_flushes_remaining_rows(service, mock_part_flusher):
    service.run_discovery()
    mock_part_flusher.flush_if_pending.assert_called()


# --- multi-page traversal ---

def test_run_discovery_follows_cursor_until_exhausted(service, mock_hf_client, mock_hf_types):
    mock_hf_client.fetch_models_page.side_effect = [
        ([{"modelId": "org/m1"}], CURSOR, {}),
        ([{"modelId": "org/m2"}], None, {}),
    ]
    mock_hf_types.filter_page.return_value = ([MODEL_ROW], 1)

    service.run_discovery()

    assert mock_hf_client.fetch_models_page.call_count == 2


def test_run_discovery_breaks_on_empty_page(service, mock_hf_client, mock_event_logger):
    mock_hf_client.fetch_models_page.return_value = ([], None, {})

    service.run_discovery()

    mock_event_logger.log_empty_page.assert_called_once()


# --- rate limiting delegation ---

def test_run_discovery_delegates_rate_limit_to_handler(service, mock_hf_client, mock_rate_limit_handler):
    rate_exc = RateLimitError(429, "60", "Too Many Requests", {})
    # First call raises 429, second call succeeds and ends the loop
    mock_hf_client.fetch_models_page.side_effect = [
        rate_exc,
        ([{"modelId": "org/m1"}], None, {}),
    ]

    service.run_discovery()

    mock_rate_limit_handler.handle.assert_called_once()
    exc_passed = mock_rate_limit_handler.handle.call_args.args[0]
    assert exc_passed is rate_exc


# --- failure path ---

def test_run_discovery_saves_failed_checkpoint_on_error(service, mock_hf_client, mock_checkpoint_service):
    mock_hf_client.fetch_models_page.side_effect = RuntimeError("network failure")

    with pytest.raises(RuntimeError, match="network failure"):
        service.run_discovery()

    saved_checkpoints = [
        c.args[0] for c in mock_checkpoint_service.save_checkpoint.call_args_list
    ]
    assert any(cp["status"] == "FAILED" for cp in saved_checkpoints)


def test_run_discovery_saves_error_progress_event_on_failure(service, mock_hf_client, mock_checkpoint_service):
    mock_hf_client.fetch_models_page.side_effect = RuntimeError("boom")

    with pytest.raises(RuntimeError):
        service.run_discovery()

    event_names = [c.args[1] for c in mock_checkpoint_service.save_progress_event.call_args_list]
    assert "error" in event_names


def test_run_discovery_logs_failed_on_exception(service, mock_hf_client, mock_event_logger):
    mock_hf_client.fetch_models_page.side_effect = RuntimeError("crash")

    with pytest.raises(RuntimeError):
        service.run_discovery()

    mock_event_logger.log_failed.assert_called_once()


def test_run_discovery_flushes_pending_rows_on_failure(service, mock_hf_client, mock_part_flusher):
    mock_hf_client.fetch_models_page.side_effect = RuntimeError("error mid-run")

    with pytest.raises(RuntimeError):
        service.run_discovery()

    mock_part_flusher.flush_if_pending.assert_called()


# --- checkpoint periodic save ---

def test_run_discovery_saves_checkpoint_every_10_pages(service, mock_hf_client, mock_hf_types, mock_checkpoint_service):
    pages = [([{"modelId": f"org/m{i}"}], f"cursor_{i+1}", {}) for i in range(9)]
    pages.append(([{"modelId": "org/m9"}], None, {}))
    mock_hf_client.fetch_models_page.side_effect = pages
    mock_hf_types.filter_page.return_value = ([], 0)

    service.run_discovery()

    # page 10 (index 9, page_number becomes 10 which is divisible by 10) should trigger a save
    save_count = mock_checkpoint_service.save_checkpoint.call_count
    assert save_count >= 2
