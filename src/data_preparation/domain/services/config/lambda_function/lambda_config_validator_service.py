from typing import Protocol, Any

from data_preparation.domain.models.preparation.input_manifest import InputManifest


class LambdaConfigValidatorService(Protocol):
    def validate_input(self, config: InputManifest) -> None:
        ...
