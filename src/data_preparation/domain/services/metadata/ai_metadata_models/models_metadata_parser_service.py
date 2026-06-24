from typing import Any, Protocol


class AIModelsMetadataParserService(Protocol):
    def safe_float(self, value: Any) -> float | None:
        ...

    def safe_int(self, value: Any) -> int:
        ...
