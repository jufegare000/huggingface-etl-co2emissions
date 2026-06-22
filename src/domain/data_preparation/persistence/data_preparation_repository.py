from typing import Any, Protocol


class DataPreparationRepository(Protocol):
    def persist_preparation_output(
            self,
            partitions: list[dict[str, Any]],
            bucket: str,
            config: dict[str, Any],
            manifest: dict[str, str | int],
            manifest_key: str
    ) -> dict[str, Any]:
        ...
