from typing import Protocol, Dict, Any


class LambdaConfigService(Protocol):

    def load_input_manifest(self, event: Dict[str, Any]) -> Dict[str, Any]:
        ...
