from __future__ import annotations
import dataclasses
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


@dataclass
class DiscoveryRunState:
    snapshot_id: str
    discovered_at: str
    next_cursor: Optional[str]
    page_number: int
    total_models_seen: int
    models_with_emissions: int
    part_number: int
    pending_rows: List[Dict[str, Any]] = field(default_factory=list)
    rate_limit_retries: int = 0

    @classmethod
    def from_checkpoint(cls, checkpoint: Dict[str, Any], fallback_now: str) -> DiscoveryRunState:
        return cls(
            snapshot_id=checkpoint["snapshot_id"],
            discovered_at=checkpoint.get("started_at") or fallback_now,
            next_cursor=checkpoint.get("next_cursor"),
            page_number=int(checkpoint.get("last_successful_page") or 0),
            total_models_seen=int(checkpoint.get("total_models_seen") or 0),
            models_with_emissions=int(checkpoint.get("models_with_emissions") or 0),
            part_number=int(checkpoint.get("part_number") or 0),
        )

    def apply_to_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        checkpoint["next_cursor"] = self.next_cursor
        checkpoint["last_successful_page"] = self.page_number
        checkpoint["total_models_seen"] = self.total_models_seen
        checkpoint["models_with_emissions"] = self.models_with_emissions
        checkpoint["part_number"] = self.part_number

    def to_error_payload(self, error_message: str, now_iso: str) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "status": "FAILED",
            "error_message": error_message,
            "total_models_seen": self.total_models_seen,
            "models_with_emissions": self.models_with_emissions,
            "page_number": self.page_number,
            "next_cursor_saved": bool(self.next_cursor),
            "failed_at": now_iso,
        }

    def to_rate_limited_payload(self, sleep_seconds: int, retry_after: Optional[str], now_iso: str) -> Dict[str, Any]:
        return {
            "snapshot_id": self.snapshot_id,
            "status": "RATE_LIMITED",
            "retry_after": retry_after,
            "sleep_seconds": sleep_seconds,
            "total_models_seen": self.total_models_seen,
            "models_with_emissions": self.models_with_emissions,
            "page_number": self.page_number,
            "cursor_saved": bool(self.next_cursor),
            "event_at": now_iso,
        }
