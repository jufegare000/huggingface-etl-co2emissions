from typing import Any, Protocol

from domain.data_preparation.models.data.final_manifest import FinalManifest
from domain.data_preparation.models.data.input_manifest import InputManifest
from domain.data_preparation.models.data.partition_descriptor import PartitionDescriptor
from domain.data_preparation.models.data.persistence_structure import PersistenceStructure


class DataPreparationRepository(Protocol):
    def persist_preparation_output(
            self,
            partitions: list[PartitionDescriptor],
            bucket: str,
            config: InputManifest,
            manifest: FinalManifest,
            manifest_key: str
    ) -> PersistenceStructure:
        ...
