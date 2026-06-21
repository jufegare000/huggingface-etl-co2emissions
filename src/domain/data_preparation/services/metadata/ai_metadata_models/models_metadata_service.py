from typing import Protocol
from typing import Any, Dict, List

class AIModelsMetadataService(Protocol):

    def load_models_metadata(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        ...
