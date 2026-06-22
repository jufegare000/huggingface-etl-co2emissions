from dataclasses import dataclass

from domain.data_preparation.models.preparation.partition_status import PartitionStatus
from domain.shared.models.serializable_model import SerializableModel


@dataclass
class PartitionDescriptor(SerializableModel):
    partition_id: str
    input_path: str
    thread_count: int
    records_count: int
    emission_min: float | None
    emission_max: float | None
    status: PartitionStatus
