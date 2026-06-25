from data_discovery.domain.models.discovery_run_state import DiscoveryRunState

SNAPSHOT_ID = "20240101T120000"
STARTED_AT = "2024-01-01T12:00:00Z"
FALLBACK_NOW = "2024-01-01T13:00:00Z"
CURSOR = "cursor_xyz"


def _make_checkpoint(**overrides):
    base = {
        "snapshot_id": SNAPSHOT_ID,
        "started_at": STARTED_AT,
        "next_cursor": None,
        "last_successful_page": 0,
        "total_models_seen": 0,
        "models_with_emissions": 0,
        "part_number": 0,
    }
    base.update(overrides)
    return base


# --- from_checkpoint ---

def test_from_checkpoint_reads_all_fields():
    cp = _make_checkpoint(
        next_cursor=CURSOR,
        last_successful_page=7,
        total_models_seen=7000,
        models_with_emissions=50,
        part_number=3,
    )
    state = DiscoveryRunState.from_checkpoint(cp, FALLBACK_NOW)

    assert state.snapshot_id == SNAPSHOT_ID
    assert state.discovered_at == STARTED_AT
    assert state.next_cursor == CURSOR
    assert state.page_number == 7
    assert state.total_models_seen == 7000
    assert state.models_with_emissions == 50
    assert state.part_number == 3
    assert state.rate_limit_retries == 0


def test_from_checkpoint_uses_fallback_when_started_at_missing():
    cp = _make_checkpoint()
    del cp["started_at"]
    state = DiscoveryRunState.from_checkpoint(cp, FALLBACK_NOW)
    assert state.discovered_at == FALLBACK_NOW


def test_from_checkpoint_coerces_string_numbers():
    cp = _make_checkpoint(
        last_successful_page="4",
        total_models_seen="2000",
        models_with_emissions="8",
        part_number="1",
    )
    state = DiscoveryRunState.from_checkpoint(cp, FALLBACK_NOW)
    assert state.page_number == 4
    assert state.total_models_seen == 2000
    assert state.models_with_emissions == 8
    assert state.part_number == 1


def test_from_checkpoint_defaults_numeric_fields_when_none():
    cp = {
        "snapshot_id": SNAPSHOT_ID,
        "started_at": STARTED_AT,
    }
    state = DiscoveryRunState.from_checkpoint(cp, FALLBACK_NOW)
    assert state.page_number == 0
    assert state.total_models_seen == 0
    assert state.models_with_emissions == 0
    assert state.part_number == 0
    assert state.next_cursor is None


# --- apply_to_checkpoint ---

def test_apply_to_checkpoint_writes_all_fields():
    state = DiscoveryRunState(
        snapshot_id=SNAPSHOT_ID,
        discovered_at=STARTED_AT,
        next_cursor=CURSOR,
        page_number=5,
        total_models_seen=5000,
        models_with_emissions=20,
        part_number=2,
    )
    cp = {}
    state.apply_to_checkpoint(cp)

    assert cp["next_cursor"] == CURSOR
    assert cp["last_successful_page"] == 5
    assert cp["total_models_seen"] == 5000
    assert cp["models_with_emissions"] == 20
    assert cp["part_number"] == 2


def test_apply_to_checkpoint_overwrites_existing_values():
    state = DiscoveryRunState(
        snapshot_id=SNAPSHOT_ID,
        discovered_at=STARTED_AT,
        next_cursor=None,
        page_number=10,
        total_models_seen=10000,
        models_with_emissions=0,
        part_number=5,
    )
    cp = {"last_successful_page": 1, "total_models_seen": 999}
    state.apply_to_checkpoint(cp)

    assert cp["last_successful_page"] == 10
    assert cp["total_models_seen"] == 10000


# --- to_error_payload ---

def test_to_error_payload_structure():
    state = DiscoveryRunState(
        snapshot_id=SNAPSHOT_ID,
        discovered_at=STARTED_AT,
        next_cursor=CURSOR,
        page_number=3,
        total_models_seen=3000,
        models_with_emissions=15,
        part_number=1,
    )
    now = "2024-01-01T14:00:00Z"
    payload = state.to_error_payload("something went wrong", now)

    assert payload["snapshot_id"] == SNAPSHOT_ID
    assert payload["status"] == "FAILED"
    assert payload["error_message"] == "something went wrong"
    assert payload["total_models_seen"] == 3000
    assert payload["models_with_emissions"] == 15
    assert payload["page_number"] == 3
    assert payload["next_cursor_saved"] is True
    assert payload["failed_at"] == now


def test_to_error_payload_no_cursor_flag():
    state = DiscoveryRunState(
        snapshot_id=SNAPSHOT_ID,
        discovered_at=STARTED_AT,
        next_cursor=None,
        page_number=0,
        total_models_seen=0,
        models_with_emissions=0,
        part_number=0,
    )
    payload = state.to_error_payload("oops", "2024-01-01T00:00:00Z")
    assert payload["next_cursor_saved"] is False


# --- to_rate_limited_payload ---

def test_to_rate_limited_payload_structure():
    state = DiscoveryRunState(
        snapshot_id=SNAPSHOT_ID,
        discovered_at=STARTED_AT,
        next_cursor=CURSOR,
        page_number=2,
        total_models_seen=2000,
        models_with_emissions=5,
        part_number=1,
    )
    now = "2024-01-01T13:30:00Z"
    payload = state.to_rate_limited_payload(sleep_seconds=60, retry_after="60", now_iso=now)

    assert payload["snapshot_id"] == SNAPSHOT_ID
    assert payload["status"] == "RATE_LIMITED"
    assert payload["retry_after"] == "60"
    assert payload["sleep_seconds"] == 60
    assert payload["total_models_seen"] == 2000
    assert payload["models_with_emissions"] == 5
    assert payload["page_number"] == 2
    assert payload["cursor_saved"] is True
    assert payload["event_at"] == now
