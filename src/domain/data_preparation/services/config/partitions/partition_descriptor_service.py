from typing import Protocol

from domain.data_preparation.models.data.boundary import Boundary
from domain.data_preparation.models.data.input_manifest import InputManifest
from domain.data_preparation.models.data.model_metadata import ModelMetadata
from domain.data_preparation.models.data.partition_descriptor import PartitionDescriptor


class PartitionDescriptorService(Protocol):
    def build_partition_descriptors(
            self,
            boundaries: list[Boundary],
            config: InputManifest,
            models: list[ModelMetadata],
    ) -> list[PartitionDescriptor]:
        ...
