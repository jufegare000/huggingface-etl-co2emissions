from dataclasses import dataclass

from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor
from domain.shared.models.serializable_model import SerializableModel


@dataclass
class StepFunctionOutput(SerializableModel):
    run_id: str
    bucket_name: str
    control_table_name: str
    source_csv_path: str
    manifest_path: str
    partitions_count: int
    partitions: list[PartitionDescriptor]