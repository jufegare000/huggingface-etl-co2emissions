from dataclasses import dataclass

@dataclass(frozen=True)
class CheckpointCreatedPayload:
    snapshot_id: str
    event: str = "checkpoint_created"