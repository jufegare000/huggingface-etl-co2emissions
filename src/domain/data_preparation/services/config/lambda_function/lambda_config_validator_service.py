from typing import Protocol, Any

from domain.data_preparation.models.preparation.input_manifest import InputManifest


class LambdaConfigValidatorService(Protocol):
    def validate_input(self, config: InputManifest) -> None:
        ...
