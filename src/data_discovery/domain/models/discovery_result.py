from __future__ import annotations
import dataclasses
from dataclasses import dataclass
from typing import Dict, Any

from data_discovery.domain.models.discovery_run_state import DiscoveryRunState


@dataclass
class DiscoveryResult:
    snapshot_id: str
    status: str
    snapshot_path: str
    latest_path: str
    total_models_seen: int
    models_with_emissions: int
    final_rows_count: int
    part_number: int
    completed_at: str

    @classmethod
    def from_state(
        cls,
        state: DiscoveryRunState,
        snapshot_path: str,
        final_rows_count: int,
        latest_path: str,
        completed_at: str,
    ) -> DiscoveryResult:
        return cls(
            snapshot_id=state.snapshot_id,
            status="COMPLETED",
            snapshot_path=snapshot_path,
            latest_path=latest_path,
            total_models_seen=state.total_models_seen,
            models_with_emissions=state.models_with_emissions,
            final_rows_count=final_rows_count,
            part_number=state.part_number,
            completed_at=completed_at,
        )

    def to_dict(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)
