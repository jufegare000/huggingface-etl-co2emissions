from typing import Protocol

from data_preparation.domain.models.preparation.input_manifest import InputManifest
from data_preparation.domain.models.preparation.model_metadata import ModelMetadata


class AIModelsMetadataService(Protocol):

    def load_models_metadata(self, config: InputManifest) -> list[ModelMetadata]:
        ...
