from dataclasses import dataclass
from typing import Optional


@dataclass
class RecuperationMetrics:
    run_id: str
    errors_scanned: int = 0
    rate_limit_errors_found: int = 0
    recuperated_count: int = 0
    still_failed_count: int = 0
    api_calls_count: int = 0
    partitions_processed: int = 0
    completed_at: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "errors_scanned": self.errors_scanned,
            "rate_limit_errors_found": self.rate_limit_errors_found,
            "recuperated_count": self.recuperated_count,
            "still_failed_count": self.still_failed_count,
            "api_calls_count": self.api_calls_count,
            "partitions_processed": self.partitions_processed,
            "completed_at": self.completed_at,
        }
