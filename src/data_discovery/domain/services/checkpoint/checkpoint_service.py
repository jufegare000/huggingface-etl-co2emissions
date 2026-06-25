from typing import Protocol, Dict, Any


class CheckpointService(Protocol):
    def load_or_create_checkpoint(self) -> Dict[str, Any]:
        ...

    def save_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        ...

    def save_progress_event(self, snapshot_id: str, event_name: str, payload: Dict[str, Any]) -> None:
        ...
