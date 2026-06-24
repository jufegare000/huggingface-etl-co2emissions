from typing import Protocol

from domain.data_preparation.models.preparation.final_manifest import FinalManifest
from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor


class DataManifestBuilderService(Protocol):
    def build_manifest(
            self,
            partitions: list[PartitionDescriptor],
            bucket: str,
            config: InputManifest,
    ) -> tuple[FinalManifest, str]:
        ...
