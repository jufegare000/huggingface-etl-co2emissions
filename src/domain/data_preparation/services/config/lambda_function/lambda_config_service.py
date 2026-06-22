from typing import Protocol, Any


class LambdaConfigService(Protocol):

    def load_input_manifest(self, event: dict[str, Any]) -> dict[str, Any]:
        ...
