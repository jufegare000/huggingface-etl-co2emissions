from typing import Any, Protocol


class BoundariesCalculationService(Protocol):
    def calculate_percentile_boundaries(
            self,
            models: list[dict[str, Any]],
            workers: int,
    ) -> list[dict[str, Any]]:
        ...
