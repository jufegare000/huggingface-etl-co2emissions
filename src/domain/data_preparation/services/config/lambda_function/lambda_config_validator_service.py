from typing import Protocol, Dict, Any


class LambdaConfigValidatorService(Protocol):
    def validate_input(self, config: Dict[str, Any]) -> None:
        ...
