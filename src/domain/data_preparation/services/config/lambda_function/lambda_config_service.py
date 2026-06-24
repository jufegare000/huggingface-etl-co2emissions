from typing import Protocol, Any
from domain.data_preparation.models.preparation.input_manifest import InputManifest


class LambdaConfigService(Protocol):

    def load_input_manifest(self) -> InputManifest:
        ...
