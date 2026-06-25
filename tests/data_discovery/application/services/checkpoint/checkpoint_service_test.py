from unittest.mock import MagicMock, call

import pytest

from data_discovery.application.services.checkpoint.checkpoint_service_implemented import (
    CheckPointServiceImplemented,
)
from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum

BUCKET = "my-discovery-bucket"
SNAPSHOT_ID = "20240101T120000"
COMPACT_NOW = SNAPSHOT_ID
ISO_NOW = "2024-01-01T12:00:00Z"
ISO_UPDATED = "2024-01-01T12:05:00Z"


def _base_checkpoint_dict():
    return {
        "snapshot_id": SNAPSHOT_ID,
        "status": "RUNNING",
        "next_cursor": None,
        "last_successful_page": 0,
        "total_models_seen": 0,
        "models_with_emissions": 0,
        "part_number": 0,
        "started_at": ISO_NOW,
        "updated_at": ISO_NOW,
    }


@pytest.fixture
def mock_reader():
    return MagicMock()


@pytest.fixture
def mock_writer():
    return MagicMock()


@pytest.fixture
def mock_date_service():
    m = MagicMock()
    m.utc_now_iso.return_value = ISO_NOW
    m.utc_now_compact.return_value = COMPACT_NOW
    return m


@pytest.fixture
def service(mock_reader, mock_writer, mock_date_service):
    return CheckPointServiceImplemented(
        s3_json_reader_service=mock_reader,
        s3_writer_service=mock_writer,
        date_parsing_service=mock_date_service,
        target_bucket=BUCKET,
    )


# --- load_or_create_checkpoint: resume path ---

def test_load_or_create_returns_existing_resumable_checkpoint(service, mock_reader):
    existing = _base_checkpoint_dict()
    mock_reader.read_json_from_s3.return_value = existing

    result = service.load_or_create_checkpoint()

    assert result == existing
    mock_reader.read_json_from_s3.assert_called_once_with(BUCKET, DiscoveryJobConstantsEnum.CHECKPOINT_KEY)


def test_load_or_create_does_not_overwrite_when_resumable(service, mock_reader, mock_writer):
    mock_reader.read_json_from_s3.return_value = _base_checkpoint_dict()
    service.load_or_create_checkpoint()
    mock_writer.write_json_to_s3.assert_not_called()


# --- load_or_create_checkpoint: fresh path ---

def test_load_or_create_creates_new_when_no_existing_checkpoint(service, mock_reader, mock_writer):
    mock_reader.read_json_from_s3.return_value = None

    result = service.load_or_create_checkpoint()

    assert result["snapshot_id"] == SNAPSHOT_ID
    assert result["status"] == "RUNNING"
    mock_writer.write_json_to_s3.assert_called()


def test_load_or_create_creates_new_when_completed_checkpoint(service, mock_reader, mock_writer):
    completed = _base_checkpoint_dict()
    completed["status"] = "COMPLETED"
    mock_reader.read_json_from_s3.return_value = completed

    result = service.load_or_create_checkpoint()

    assert result["status"] == "RUNNING"
    mock_writer.write_json_to_s3.assert_called()


# --- save_checkpoint ---

def test_save_checkpoint_stamps_updated_at(service, mock_writer, mock_date_service):
    mock_date_service.utc_now_iso.return_value = ISO_UPDATED
    cp = _base_checkpoint_dict()
    service.save_checkpoint(cp)

    assert cp["updated_at"] == ISO_UPDATED


def test_save_checkpoint_writes_to_two_s3_paths(service, mock_writer):
    cp = _base_checkpoint_dict()
    service.save_checkpoint(cp)

    assert mock_writer.write_json_to_s3.call_count == 2
    called_keys = [c.args[2] for c in mock_writer.write_json_to_s3.call_args_list]
    assert DiscoveryJobConstantsEnum.CHECKPOINT_KEY in called_keys
    snapshot_key = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={SNAPSHOT_ID}/_metadata/checkpoint.json"
    assert snapshot_key in called_keys


# --- save_progress_event ---

def test_save_progress_event_writes_correct_key(service, mock_writer):
    payload = {"some": "data"}
    service.save_progress_event(SNAPSHOT_ID, "result", payload)

    expected_key = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={SNAPSHOT_ID}/_metadata/result.json"
    mock_writer.write_json_to_s3.assert_called_once_with(payload, BUCKET, expected_key)


def test_save_progress_event_uses_event_name_in_key(service, mock_writer):
    service.save_progress_event(SNAPSHOT_ID, "rate_limited", {})
    key_used = mock_writer.write_json_to_s3.call_args.args[2]
    assert "rate_limited" in key_used
