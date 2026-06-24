from dataclasses import dataclass
from typing import List

from data_preparation.domain.models.preparation.manifest_status import ManifestStatus
from data_preparation.domain.models.preparation.partition_descriptor import PartitionDescriptor
from shared.domain.models.serializable_model import SerializableModel


@dataclass
class FinalManifest(SerializableModel):
    run_id: str
    status: ManifestStatus
    source_csv_path: str
    manifest_path: str
    partitions_count: int
    workers: int
    threads_per_worker: int
    global_rate_limit: int
    window_seconds: int
    calls_per_model: int
    created_at: str
    partitions: List[PartitionDescriptor]
