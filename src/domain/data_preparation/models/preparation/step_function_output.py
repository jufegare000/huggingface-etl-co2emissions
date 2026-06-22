from dataclasses import dataclass, asdict
from typing import List

from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor


@dataclass
class StepFunctionOutput:
    run_id: str
    bucket_name: str
    control_table_name: str
    source_csv_path: str
    manifest_path: str
    partitions_count: int
    partitions: List[PartitionDescriptor]

    def to_dict(self) -> dict:
        return asdict(self)
