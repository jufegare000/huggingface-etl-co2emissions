from typing import Protocol

from domain.data_preparation.models.preparation.boundary import Boundary
from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.model_metadata import ModelMetadata
from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor


class PartitionDescriptorService(Protocol):
    def build_partition_descriptors(
            self,
            boundaries: list[Boundary],
            config: InputManifest,
            models: list[ModelMetadata],
    ) -> list[PartitionDescriptor]:
        ...
