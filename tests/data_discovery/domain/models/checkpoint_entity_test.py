import pytest

from data_discovery.domain.models.checkpoint_entity import CheckpointEntity
from data_discovery.domain.models.resumable_statuses_enum import CheckpointStatusEnum

SNAPSHOT_ID = "20240101T120000"
STARTED_AT = "2024-01-01T12:00:00Z"
UPDATED_AT = "2024-01-01T12:05:00Z"
CURSOR = "abc123"


# --- CheckpointStatusEnum ---

def test_running_is_resumable():
    assert CheckpointStatusEnum.RUNNING.is_resumable is True


def test_failed_is_resumable():
    assert CheckpointStatusEnum.FAILED.is_resumable is True


def test_rate_limited_is_resumable():
    assert CheckpointStatusEnum.RATE_LIMITED.is_resumable is True


def test_interrupted_is_resumable():
    assert CheckpointStatusEnum.INTERRUPTED.is_resumable is True


def test_completed_is_not_resumable():
    assert CheckpointStatusEnum.COMPLETED.is_resumable is False


# --- CheckpointEntity.new ---

def test_new_creates_entity_with_defaults():
    entity = CheckpointEntity.new(snapshot_id=SNAPSHOT_ID, started_at=STARTED_AT)

    assert entity.snapshot_id == SNAPSHOT_ID
    assert entity.status == CheckpointStatusEnum.RUNNING
    assert entity.next_cursor is None
    assert entity.last_successful_page == 0
    assert entity.total_models_seen == 0
    assert entity.models_with_emissions == 0
    assert entity.part_number == 0
    assert entity.started_at == STARTED_AT
    assert entity.updated_at == STARTED_AT


# --- CheckpointEntity.from_dict ---

def test_from_dict_full_payload():
    data = {
        "snapshot_id": SNAPSHOT_ID,
        "status": "RUNNING",
        "next_cursor": CURSOR,
        "last_successful_page": 5,
        "total_models_seen": 5000,
        "models_with_emissions": 42,
        "part_number": 2,
        "started_at": STARTED_AT,
        "updated_at": UPDATED_AT,
    }
    entity = CheckpointEntity.from_dict(data)

    assert entity.snapshot_id == SNAPSHOT_ID
    assert entity.status == CheckpointStatusEnum.RUNNING
    assert entity.next_cursor == CURSOR
    assert entity.last_successful_page == 5
    assert entity.total_models_seen == 5000
    assert entity.models_with_emissions == 42
    assert entity.part_number == 2


def test_from_dict_missing_optional_fields_default_to_zero():
    data = {
        "snapshot_id": SNAPSHOT_ID,
        "status": "COMPLETED",
        "started_at": STARTED_AT,
        "updated_at": UPDATED_AT,
    }
    entity = CheckpointEntity.from_dict(data)

    assert entity.next_cursor is None
    assert entity.last_successful_page == 0
    assert entity.total_models_seen == 0
    assert entity.models_with_emissions == 0
    assert entity.part_number == 0


def test_from_dict_unknown_status_raises():
    with pytest.raises(ValueError):
        CheckpointEntity.from_dict({
            "snapshot_id": SNAPSHOT_ID,
            "status": "BOGUS",
            "started_at": STARTED_AT,
            "updated_at": UPDATED_AT,
        })


# --- CheckpointEntity.is_resumable ---

def test_is_resumable_delegates_to_status():
    entity = CheckpointEntity.new(snapshot_id=SNAPSHOT_ID, started_at=STARTED_AT)
    assert entity.is_resumable == entity.status.is_resumable


# --- CheckpointEntity.to_dict ---

def test_to_dict_serializes_status_as_string():
    entity = CheckpointEntity.new(snapshot_id=SNAPSHOT_ID, started_at=STARTED_AT)
    result = entity.to_dict()

    assert result["status"] == "RUNNING"
    assert isinstance(result["status"], str)
    assert result["snapshot_id"] == SNAPSHOT_ID


# --- CheckpointEntity.resume_log_payload ---

def test_resume_log_payload_structure():
    entity = CheckpointEntity.new(snapshot_id=SNAPSHOT_ID, started_at=STARTED_AT)
    entity.next_cursor = CURSOR
    entity.last_successful_page = 3
    entity.total_models_seen = 3000
    entity.models_with_emissions = 10

    payload = entity.resume_log_payload()

    assert payload["event"] == "checkpoint_found"
    assert payload["snapshot_id"] == SNAPSHOT_ID
    assert payload["next_cursor_present"] is True
    assert payload["last_successful_page"] == 3
    assert payload["total_models_seen"] == 3000
    assert payload["models_with_emissions"] == 10


def test_resume_log_payload_no_cursor():
    entity = CheckpointEntity.new(snapshot_id=SNAPSHOT_ID, started_at=STARTED_AT)
    payload = entity.resume_log_payload()
    assert payload["next_cursor_present"] is False


# --- CheckpointEntity.created_log_payload ---

def test_created_log_payload_structure():
    entity = CheckpointEntity.new(snapshot_id=SNAPSHOT_ID, started_at=STARTED_AT)
    payload = entity.created_log_payload()

    assert payload["event"] == "checkpoint_created"
    assert payload["snapshot_id"] == SNAPSHOT_ID
