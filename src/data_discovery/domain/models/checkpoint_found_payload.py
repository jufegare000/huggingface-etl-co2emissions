from dataclasses import dataclass


@dataclass(frozen=True)

class CheckpointFoundPayload:
    snapshot_id: str
    next_cursor_present: bool
    last_successful_page: int
    total_models_seen: int
    models_with_emissions: int
    event: str = "checkpoint_found"
