from typing import Protocol
from typing import Any

class AIModelsMetadataService(Protocol):

    def load_models_metadata(self, config: dict[str, Any]) -> list[dict[str, Any]]:
        ...
