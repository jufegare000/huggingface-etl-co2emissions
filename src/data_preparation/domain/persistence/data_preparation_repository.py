from typing import Protocol

from data_preparation.domain.models.preparation.final_manifest import FinalManifest
from data_preparation.domain.models.preparation.input_manifest import InputManifest
from data_preparation.domain.models.preparation.persistence_structure import PersistenceStructure


class DataPreparationRepository(Protocol):
    def persist_preparation_output(
            self,
            bucket: str,
            config: InputManifest,
            manifest: FinalManifest,
            manifest_key: str
    ) -> PersistenceStructure:
        ...
