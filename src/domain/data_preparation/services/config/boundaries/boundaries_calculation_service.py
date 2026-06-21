from typing import Any, Protocol, Dict, List


class BoundariesCalculationService(Protocol):
    def calculate_percentile_boundaries(
            self,
            models: List[Dict[str, Any]],
            workers: int,
    ) -> List[Dict[str, Any]]:
        ...
