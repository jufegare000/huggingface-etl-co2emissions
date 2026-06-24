from dataclasses import dataclass

from data_preparation.domain.models.preparation.input_manifest import InputManifest
from data_preparation.domain.models.preparation.partition_descriptor import PartitionDescriptor
from data_preparation.domain.models.preparation.persistence_structure import PersistenceStructure
from shared.domain.models.serializable_model import SerializableModel


@dataclass
class StepFunctionInput(SerializableModel):
    partitions: list[PartitionDescriptor]
    persistence_result: PersistenceStructure
    bucket_name: str
    input_manifest: InputManifest
