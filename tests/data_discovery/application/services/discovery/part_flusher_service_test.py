from unittest.mock import MagicMock, call

import pytest

from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from data_discovery.application.services.discovery.part_flusher_service_implemented import (
    PartFlusherServiceImplemented,
)
from data_discovery.domain.models.discovery_run_state import DiscoveryRunState

SNAPSHOT_ID = "20240101T120000"
STARTED_AT = "2024-01-01T12:00:00Z"
PART_URI = "s3://bucket/discovery/hf-carbon/snapshot_id=20240101T120000/filtered_parts/models_with_emissions_part_000001.csv"

ROW = {"model_id": "org/model", "co2_eq_emissions": "1.5"}


def _make_state(**overrides):
    defaults = dict(
        snapshot_id=SNAPSHOT_ID,
        discovered_at=STARTED_AT,
        next_cursor=None,
        page_number=1,
        total_models_seen=100,
        models_with_emissions=1,
        part_number=0,
        pending_rows=[],
    )
    defaults.update(overrides)
    return DiscoveryRunState(**defaults)


@pytest.fixture
def mock_uploader():
    m = MagicMock()
    m.upload_rows_part.return_value = PART_URI
    return m


@pytest.fixture
def mock_checkpoint_service():
    return MagicMock()


@pytest.fixture
def mock_event_logger():
    return MagicMock()


@pytest.fixture
def flusher(mock_uploader, mock_checkpoint_service, mock_event_logger):
    return PartFlusherServiceImplemented(
        uploader_process_service=mock_uploader,
        checkpoint_service=mock_checkpoint_service,
        event_logger=mock_event_logger,
    )


# --- flush_if_pending ---

def test_flush_if_pending_does_nothing_when_empty(flusher, mock_uploader, mock_checkpoint_service):
    state = _make_state(pending_rows=[])
    cp = {}
    flusher.flush_if_pending(state, cp)

    mock_uploader.upload_rows_part.assert_not_called()
    mock_checkpoint_service.save_checkpoint.assert_not_called()


def test_flush_if_pending_uploads_and_clears_rows(flusher, mock_uploader):
    state = _make_state(pending_rows=[ROW], part_number=0)
    cp = {"snapshot_id": SNAPSHOT_ID}
    flusher.flush_if_pending(state, cp)

    mock_uploader.upload_rows_part.assert_called_once_with(SNAPSHOT_ID, 1, [ROW])
    assert state.pending_rows == []
    assert state.part_number == 1


def test_flush_if_pending_saves_checkpoint(flusher, mock_checkpoint_service):
    state = _make_state(pending_rows=[ROW])
    cp = {}
    flusher.flush_if_pending(state, cp)

    mock_checkpoint_service.save_checkpoint.assert_called_once()
    assert cp["last_part_uri"] == PART_URI


def test_flush_if_pending_logs_event(flusher, mock_event_logger):
    state = _make_state(pending_rows=[ROW])
    flusher.flush_if_pending(state, {})
    mock_event_logger.log_part_flushed.assert_called_once_with(state, PART_URI)


# --- flush_on_condition ---

def test_flush_on_condition_skips_when_no_pending_rows(flusher, mock_uploader):
    state = _make_state(pending_rows=[], page_number=DiscoveryJobConstantsEnum.FLUSH_EVERY_PAGES)
    flusher.flush_on_condition(state, {})
    mock_uploader.upload_rows_part.assert_not_called()


def test_flush_on_condition_flushes_at_page_interval(flusher, mock_uploader):
    state = _make_state(
        pending_rows=[ROW],
        page_number=DiscoveryJobConstantsEnum.FLUSH_EVERY_PAGES,
    )
    flusher.flush_on_condition(state, {})
    mock_uploader.upload_rows_part.assert_called_once()


def test_flush_on_condition_flushes_when_match_threshold_reached(flusher, mock_uploader):
    rows = [ROW] * DiscoveryJobConstantsEnum.FLUSH_EVERY_MATCHES
    state = _make_state(pending_rows=rows, page_number=1)
    flusher.flush_on_condition(state, {})
    mock_uploader.upload_rows_part.assert_called_once()


def test_flush_on_condition_does_not_flush_below_thresholds(flusher, mock_uploader):
    state = _make_state(
        pending_rows=[ROW],
        page_number=DiscoveryJobConstantsEnum.FLUSH_EVERY_PAGES - 1,
    )
    flusher.flush_on_condition(state, {})
    mock_uploader.upload_rows_part.assert_not_called()
