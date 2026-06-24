from dataclasses import dataclass

from data_preparation.domain.models.preparation.partition_status import PartitionStatus
from shared.domain.models.serializable_model import SerializableModel


@dataclass
class PartitionDescriptor(SerializableModel):
    partition_id: str
    input_path: str
    thread_count: int
    records_count: int
    emission_min: float | None
    emission_max: float | None
    status: PartitionStatus
