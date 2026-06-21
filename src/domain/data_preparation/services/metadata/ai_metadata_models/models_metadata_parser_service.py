from typing import Any, Optional, Protocol


class AIModelsMetadataParserService(Protocol):
    def safe_float(self, value: Any) -> Optional[float]:
        ...

    def safe_int(self, value: Any) -> int:
        ...