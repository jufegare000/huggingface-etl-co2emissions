from dataclasses import dataclass

from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor
from domain.data_preparation.models.preparation.persistence_structure import PersistenceStructure
from domain.shared.models.serializable_model import SerializableModel


@dataclass
class StepFunctionInput(SerializableModel):
    partitions: list[PartitionDescriptor]
    persistence_result: PersistenceStructure
    bucket_name: str
    input_manifest: InputManifest
