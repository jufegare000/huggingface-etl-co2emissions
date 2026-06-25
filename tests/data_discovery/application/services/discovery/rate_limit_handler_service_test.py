from unittest.mock import MagicMock, patch

import pytest

from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from data_discovery.application.services.discovery.rate_limit_handler_service_implemented import (
    RateLimitHandlerServiceImplemented,
)
from data_discovery.domain.models.discovery_run_state import DiscoveryRunState
from shared.domain.exceptions.rate_limit_error import RateLimitError

SNAPSHOT_ID = "20240101T120000"
STARTED_AT = "2024-01-01T12:00:00Z"
NOW_ISO = "2024-01-01T12:30:00Z"
RETRY_AFTER = "60"
SLEEP_SECONDS = 60


def _make_exc(retry_after=RETRY_AFTER):
    return RateLimitError(
        status_code=429,
        retry_after=retry_after,
        response_text="Too Many Requests",
        headers={},
    )


def _make_state(rate_limit_retries=0):
    return DiscoveryRunState(
        snapshot_id=SNAPSHOT_ID,
        discovered_at=STARTED_AT,
        next_cursor="cursor_abc",
        page_number=5,
        total_models_seen=5000,
        models_with_emissions=20,
        part_number=1,
        rate_limit_retries=rate_limit_retries,
    )


@pytest.fixture
def mock_hf_client():
    m = MagicMock()
    m.retry_after_to_seconds.return_value = SLEEP_SECONDS
    return m


@pytest.fixture
def mock_checkpoint_service():
    return MagicMock()


@pytest.fixture
def mock_part_flusher():
    return MagicMock()


@pytest.fixture
def mock_event_logger():
    return MagicMock()


@pytest.fixture
def mock_date_service():
    m = MagicMock()
    m.utc_now_iso.return_value = NOW_ISO
    return m


@pytest.fixture
def handler(mock_hf_client, mock_checkpoint_service, mock_part_flusher, mock_event_logger, mock_date_service):
    return RateLimitHandlerServiceImplemented(
        hf_api_client=mock_hf_client,
        checkpoint_service=mock_checkpoint_service,
        part_flusher=mock_part_flusher,
        event_logger=mock_event_logger,
        date_parsing_service=mock_date_service,
    )


# --- handle: checkpoint update ---

def test_handle_sets_checkpoint_status_rate_limited(handler, mock_checkpoint_service):
    state = _make_state()
    cp = {}
    exc = _make_exc()
    with patch("time.sleep"):
        handler.handle(exc, state, cp)

    assert cp["status"] == "RATE_LIMITED"
    assert cp["last_retry_after"] == RETRY_AFTER
    mock_checkpoint_service.save_checkpoint.assert_called()


# --- handle: flush pending rows ---

def test_handle_flushes_pending_rows(handler, mock_part_flusher):
    state = _make_state()
    exc = _make_exc()
    cp = {}
    with patch("time.sleep"):
        handler.handle(exc, state, cp)
    mock_part_flusher.flush_if_pending.assert_called_once_with(state, cp)


# --- handle: progress event ---

def test_handle_saves_rate_limited_progress_event(handler, mock_checkpoint_service):
    state = _make_state()
    exc = _make_exc()
    with patch("time.sleep"):
        handler.handle(exc, state, {})

    mock_checkpoint_service.save_progress_event.assert_called_once()
    call_args = mock_checkpoint_service.save_progress_event.call_args
    assert call_args.kwargs["snapshot_id"] == SNAPSHOT_ID
    assert call_args.kwargs["event_name"] == "rate_limited"


# --- handle: retry counter ---

def test_handle_increments_rate_limit_retries(handler):
    state = _make_state(rate_limit_retries=0)
    with patch("time.sleep"):
        handler.handle(_make_exc(), state, {})
    assert state.rate_limit_retries == 1


# --- handle: sleep ---

def test_handle_sleeps_for_computed_seconds(handler):
    state = _make_state()
    with patch("time.sleep") as mock_sleep:
        handler.handle(_make_exc(), state, {})
    mock_sleep.assert_called_once_with(SLEEP_SECONDS)


# --- handle: log event ---

def test_handle_logs_rate_limit_sleeping(handler, mock_event_logger):
    state = _make_state()
    with patch("time.sleep"):
        handler.handle(_make_exc(), state, {})
    mock_event_logger.log_rate_limit_sleeping.assert_called_once_with(state, SLEEP_SECONDS)


# --- handle: max retries exceeded ---

def test_handle_raises_when_max_retries_exceeded(handler, mock_checkpoint_service):
    state = _make_state(rate_limit_retries=DiscoveryJobConstantsEnum.MAX_429_RETRIES_PER_RUN)
    exc = _make_exc()
    cp = {}
    with pytest.raises(RateLimitError):
        handler.handle(exc, state, cp)

    assert cp["status"] == "INTERRUPTED"
    mock_checkpoint_service.save_checkpoint.assert_called()


def test_handle_does_not_sleep_when_max_retries_exceeded(handler):
    state = _make_state(rate_limit_retries=DiscoveryJobConstantsEnum.MAX_429_RETRIES_PER_RUN)
    with patch("time.sleep") as mock_sleep:
        with pytest.raises(RateLimitError):
            handler.handle(_make_exc(), state, {})
    mock_sleep.assert_not_called()
