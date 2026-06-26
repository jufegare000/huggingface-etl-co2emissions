from dataclasses import dataclass, field
from typing import Optional


@dataclass
class IngestionMetrics:
    run_id: str
    partition_id: str
    input_count: int = 0
    processed_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    api_calls_count: int = 0
    batches_written: int = 0
    last_batch_path: Optional[str] = None
    completed_at: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "partition_id": self.partition_id,
            "input_count": self.input_count,
            "processed_count": self.processed_count,
            "success_count": self.success_count,
            "failed_count": self.failed_count,
            "api_calls_count": self.api_calls_count,
            "batches_written": self.batches_written,
            "completed_at": self.completed_at,
        }
