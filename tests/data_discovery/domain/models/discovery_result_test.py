from data_discovery.domain.models.discovery_result import DiscoveryResult
from data_discovery.domain.models.discovery_run_state import DiscoveryRunState

SNAPSHOT_ID = "20240101T120000"
STARTED_AT = "2024-01-01T12:00:00Z"
COMPLETED_AT = "2024-01-01T14:00:00Z"
SNAPSHOT_PATH = "s3://my-bucket/discovery/hf-carbon/snapshot_id=20240101T120000/filtered/models_with_emissions.csv"
LATEST_PATH = "s3://my-bucket/discovery/hf-carbon/latest/models_with_emissions.csv"


def _make_state(**overrides):
    defaults = dict(
        snapshot_id=SNAPSHOT_ID,
        discovered_at=STARTED_AT,
        next_cursor=None,
        page_number=10,
        total_models_seen=10000,
        models_with_emissions=42,
        part_number=3,
    )
    defaults.update(overrides)
    return DiscoveryRunState(**defaults)


def test_from_state_maps_all_fields():
    state = _make_state()
    result = DiscoveryResult.from_state(
        state=state,
        snapshot_path=SNAPSHOT_PATH,
        final_rows_count=38,
        latest_path=LATEST_PATH,
        completed_at=COMPLETED_AT,
    )

    assert result.snapshot_id == SNAPSHOT_ID
    assert result.status == "COMPLETED"
    assert result.snapshot_path == SNAPSHOT_PATH
    assert result.latest_path == LATEST_PATH
    assert result.total_models_seen == 10000
    assert result.models_with_emissions == 42
    assert result.final_rows_count == 38
    assert result.part_number == 3
    assert result.completed_at == COMPLETED_AT


def test_to_dict_contains_all_fields():
    state = _make_state()
    result = DiscoveryResult.from_state(
        state=state,
        snapshot_path=SNAPSHOT_PATH,
        final_rows_count=5,
        latest_path=LATEST_PATH,
        completed_at=COMPLETED_AT,
    )
    d = result.to_dict()

    assert d["status"] == "COMPLETED"
    assert d["snapshot_id"] == SNAPSHOT_ID
    assert d["snapshot_path"] == SNAPSHOT_PATH
    assert d["latest_path"] == LATEST_PATH
    assert d["total_models_seen"] == 10000
    assert d["models_with_emissions"] == 42
    assert d["final_rows_count"] == 5
    assert d["part_number"] == 3
    assert d["completed_at"] == COMPLETED_AT


def test_to_dict_is_serializable():
    state = _make_state()
    result = DiscoveryResult.from_state(
        state=state,
        snapshot_path=SNAPSHOT_PATH,
        final_rows_count=0,
        latest_path=LATEST_PATH,
        completed_at=COMPLETED_AT,
    )
    import json
    json.dumps(result.to_dict())
