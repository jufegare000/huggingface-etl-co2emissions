from typing import Any, Protocol

from domain.data_preparation.models.preparation.final_manifest import FinalManifest
from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor
from domain.data_preparation.models.preparation.persistence_structure import PersistenceStructure


class DataPreparationRepository(Protocol):
    def persist_preparation_output(
            self,
            bucket: str,
            config: InputManifest,
            manifest: FinalManifest,
            manifest_key: str
    ) -> PersistenceStructure:
        ...
