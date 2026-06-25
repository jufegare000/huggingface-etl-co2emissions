from typing import Protocol, Tuple


class SnapshotConsolidationService(Protocol):
    def consolidate_parts(self, snapshot_id: str) -> Tuple[str, int]:
        ...
