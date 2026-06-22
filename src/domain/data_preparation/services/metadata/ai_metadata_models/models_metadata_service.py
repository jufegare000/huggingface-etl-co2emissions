from typing import Protocol
from typing import Any

from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.model_metadata import ModelMetadata


class AIModelsMetadataService(Protocol):

    def load_models_metadata(self, config: InputManifest) -> list[ModelMetadata]:
        ...
