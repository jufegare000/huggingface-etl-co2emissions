from typing import Protocol

from data_preparation.domain.models.preparation.final_manifest import FinalManifest
from data_preparation.domain.models.preparation.input_manifest import InputManifest
from data_preparation.domain.models.preparation.partition_descriptor import PartitionDescriptor


class DataManifestBuilderService(Protocol):
    def build_manifest(
            self,
            partitions: list[PartitionDescriptor],
            bucket: str,
            config: InputManifest,
    ) -> tuple[FinalManifest, str]:
        ...
