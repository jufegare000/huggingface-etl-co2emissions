from typing import Any, Protocol


class DataPreparationPersistence(Protocol):
    def persist_preparation_output(
            self,
            partitions: list[dict[str, Any]],
            bucket: str,
            config: dict[str, Any],
    ) -> dict[str, Any]:
        ...
