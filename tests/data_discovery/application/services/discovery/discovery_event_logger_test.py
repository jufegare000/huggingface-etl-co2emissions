import json
import logging

import pytest

from data_discovery.application.services.discovery.discovery_event_logger_implemented import (
    DiscoveryEventLoggerImplemented,
)
from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from data_discovery.domain.models.discovery_result import DiscoveryResult
from data_discovery.domain.models.discovery_run_state import DiscoveryRunState

SNAPSHOT_ID = "20240101T120000"
STARTED_AT = "2024-01-01T12:00:00Z"


def _make_state(**overrides):
    defaults = dict(
        snapshot_id=SNAPSHOT_ID,
        discovered_at=STARTED_AT,
        next_cursor=None,
        page_number=3,
        total_models_seen=3000,
        models_with_emissions=10,
        part_number=1,
        rate_limit_retries=0,
    )
    defaults.update(overrides)
    return DiscoveryRunState(**defaults)


@pytest.fixture
def logger_impl():
    return DiscoveryEventLoggerImplemented()


def _capture_log(caplog, level, fn):
    with caplog.at_level(level, logger="data_discovery.application.services.discovery.discovery_event_logger_implemented"):
        fn()
    return caplog.records


# --- log_started ---

def test_log_started_emits_info(logger_impl, caplog):
    state = _make_state(next_cursor="cursor_abc")
    records = _capture_log(caplog, logging.INFO, lambda: logger_impl.log_started(state))

    assert len(records) == 1
    payload = json.loads(records[0].message)
    assert payload["event"] == "discovery_resumed_or_started"
    assert payload["snapshot_id"] == SNAPSHOT_ID
    assert payload["has_cursor"] is True


def test_log_started_has_cursor_false_when_none(logger_impl, caplog):
    state = _make_state(next_cursor=None)
    records = _capture_log(caplog, logging.INFO, lambda: logger_impl.log_started(state))
    payload = json.loads(records[0].message)
    assert payload["has_cursor"] is False


# --- log_empty_page ---

def test_log_empty_page_emits_info(logger_impl, caplog):
    state = _make_state()
    records = _capture_log(caplog, logging.INFO, lambda: logger_impl.log_empty_page(state))

    assert len(records) == 1
    payload = json.loads(records[0].message)
    assert payload["event"] == "empty_page_received"
    assert payload["snapshot_id"] == SNAPSHOT_ID


# --- log_part_flushed ---

def test_log_part_flushed_emits_info(logger_impl, caplog):
    state = _make_state()
    part_uri = "s3://bucket/some/path/part_000001.csv"
    records = _capture_log(caplog, logging.INFO, lambda: logger_impl.log_part_flushed(state, part_uri))

    payload = json.loads(records[0].message)
    assert payload["event"] == "part_flushed"
    assert payload["part_uri"] == part_uri
    assert payload["snapshot_id"] == SNAPSHOT_ID


# --- log_page_processed ---

def test_log_page_processed_includes_counts(logger_impl, caplog):
    state = _make_state()
    records = _capture_log(caplog, logging.INFO, lambda: logger_impl.log_page_processed(state, page_size=100, page_matches=5))

    payload = json.loads(records[0].message)
    assert payload["event"] == "page_processed"
    assert payload["page_size"] == 100
    assert payload["page_matches"] == 5
    assert payload["snapshot_id"] == SNAPSHOT_ID


# --- log_rate_limit_sleeping ---

def test_log_rate_limit_sleeping_emits_warning(logger_impl, caplog):
    state = _make_state(rate_limit_retries=2)
    records = _capture_log(caplog, logging.WARNING, lambda: logger_impl.log_rate_limit_sleeping(state, sleep_seconds=60))

    assert len(records) == 1
    assert records[0].levelno == logging.WARNING
    payload = json.loads(records[0].message)
    assert payload["event"] == "rate_limited_sleeping"
    assert payload["sleep_seconds"] == 60
    assert payload["retry"] == 2
    assert payload["max_retries"] == DiscoveryJobConstantsEnum.MAX_429_RETRIES_PER_RUN


# --- log_completed ---

def test_log_completed_emits_result_dict(logger_impl, caplog):
    state = _make_state()
    result = DiscoveryResult.from_state(
        state=state,
        snapshot_path="s3://b/snap.csv",
        final_rows_count=10,
        latest_path="s3://b/latest.csv",
        completed_at="2024-01-01T15:00:00Z",
    )
    records = _capture_log(caplog, logging.INFO, lambda: logger_impl.log_completed(result))

    payload = json.loads(records[0].message)
    assert payload["status"] == "COMPLETED"
    assert payload["snapshot_id"] == SNAPSHOT_ID


# --- log_failed ---

def test_log_failed_emits_exception_level(logger_impl, caplog):
    with caplog.at_level(logging.ERROR, logger="data_discovery.application.services.discovery.discovery_event_logger_implemented"):
        logger_impl.log_failed(SNAPSHOT_ID, "something blew up")

    assert len(caplog.records) == 1
    payload = json.loads(caplog.records[0].message)
    assert payload["event"] == "discovery_failed"
    assert payload["snapshot_id"] == SNAPSHOT_ID
    assert payload["error"] == "something blew up"
