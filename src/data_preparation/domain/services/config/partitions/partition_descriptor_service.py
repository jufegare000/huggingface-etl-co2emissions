from typing import Protocol

from data_preparation.domain.models.preparation.boundary import Boundary
from data_preparation.domain.models.preparation.input_manifest import InputManifest
from data_preparation.domain.models.preparation.model_metadata import ModelMetadata
from data_preparation.domain.models.preparation.partition_descriptor import PartitionDescriptor


class PartitionDescriptorService(Protocol):
    def build_partition_descriptors(
            self,
            boundaries: list[Boundary],
            config: InputManifest,
            models: list[ModelMetadata],
    ) -> list[PartitionDescriptor]:
        ...
