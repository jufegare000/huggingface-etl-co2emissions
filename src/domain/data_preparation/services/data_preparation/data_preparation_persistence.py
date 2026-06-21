from typing import Any, Dict, List, Protocol


class DataPreparationPersistence(Protocol):
    def persist_preparation_output(
            self,
            partitions: List[Dict[str, Any]],
            bucket: str,
            config: Dict[str, Any],
    ) -> Dict[str, Any]:
        ...
