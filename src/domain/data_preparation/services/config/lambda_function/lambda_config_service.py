from typing import Protocol, Any
from domain.data_preparation.models.data.input_manifest import InputManifest


class LambdaConfigService(Protocol):

    def load_input_manifest(self, event: dict[str, Any]) -> InputManifest:
        ...
