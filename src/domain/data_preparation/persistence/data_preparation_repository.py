from typing import Any, Dict, List, Protocol


class DataPreparationRepository(Protocol):
    def persist_preparation_output(
            self,
            partitions: List[Dict[str, Any]],
            bucket: str,
            config: Dict[str, Any],
            manifest: dict[str, str | int],
            manifest_key: str
    ) -> Dict[str, Any]:
        ...
