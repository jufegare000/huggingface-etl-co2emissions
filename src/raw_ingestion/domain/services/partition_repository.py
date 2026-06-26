from typing import Any, Dict, Optional, Protocol

from raw_ingestion.domain.models.ingestion_metrics import IngestionMetrics
from raw_ingestion.domain.models.partition_config import PartitionConfig


class PartitionRepository(Protocol):
    def get_config(self, run_id: str, partition_id: str) -> PartitionConfig:
        ...

    def mark_running(self, run_id: str, partition_id: str, job_name: str) -> None:
        ...

    def update_progress(self, run_id: str, partition_id: str, metrics: IngestionMetrics) -> None:
        ...

    def mark_completed(
        self,
        run_id: str,
        partition_id: str,
        output_prefix: str,
        success_marker_path: str,
        metrics: IngestionMetrics,
    ) -> None:
        ...

    def mark_failed(
        self,
        run_id: str,
        partition_id: str,
        error: Exception,
        metrics: IngestionMetrics,
    ) -> None:
        ...
