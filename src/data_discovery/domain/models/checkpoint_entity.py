from __future__ import annotations
from dataclasses import dataclass
import dataclasses
from typing import Optional, Dict, Any

from data_discovery.domain.models.resumable_statuses_enum import CheckpointStatusEnum


@dataclass
class CheckpointEntity:
    snapshot_id: str
    status: CheckpointStatusEnum
    next_cursor: Optional[str]
    last_successful_page: int
    total_models_seen: int
    models_with_emissions: int
    part_number: int
    started_at: str
    updated_at: str

    @classmethod
    def new(cls, snapshot_id: str, started_at: str) -> CheckpointEntity:
        return cls(
            snapshot_id=snapshot_id,
            status=CheckpointStatusEnum.RUNNING,
            next_cursor=None,
            last_successful_page=0,
            total_models_seen=0,
            models_with_emissions=0,
            part_number=0,
            started_at=started_at,
            updated_at=started_at,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CheckpointEntity:
        return cls(
            snapshot_id=data["snapshot_id"],
            status=CheckpointStatusEnum(data["status"]),
            next_cursor=data.get("next_cursor"),
            last_successful_page=data.get("last_successful_page", 0),
            total_models_seen=data.get("total_models_seen", 0),
            models_with_emissions=data.get("models_with_emissions", 0),
            part_number=data.get("part_number", 0),
            started_at=data["started_at"],
            updated_at=data["updated_at"],
        )

    @property
    def is_resumable(self) -> bool:
        return self.status.is_resumable

    def to_dict(self) -> Dict[str, Any]:
        raw = dataclasses.asdict(self)
        raw["status"] = self.status.value
        return raw

    def resume_log_payload(self) -> Dict[str, Any]:
        return {
            "event": "checkpoint_found",
            "snapshot_id": self.snapshot_id,
            "next_cursor_present": self.next_cursor is not None,
            "last_successful_page": self.last_successful_page,
            "total_models_seen": self.total_models_seen,
            "models_with_emissions": self.models_with_emissions,
        }

    def created_log_payload(self) -> Dict[str, Any]:
        return {
            "event": "checkpoint_created",
            "snapshot_id": self.snapshot_id,
        }
