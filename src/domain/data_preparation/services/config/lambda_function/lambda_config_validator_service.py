from typing import Protocol, Any


class LambdaConfigValidatorService(Protocol):
    def validate_input(self, config: dict[str, Any]) -> None:
        ...
