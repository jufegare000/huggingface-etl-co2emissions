from typing import Protocol, Any
from data_preparation.domain.models.preparation.input_manifest import InputManifest


class LambdaConfigService(Protocol):

    def load_input_manifest(self) -> InputManifest:
        ...
