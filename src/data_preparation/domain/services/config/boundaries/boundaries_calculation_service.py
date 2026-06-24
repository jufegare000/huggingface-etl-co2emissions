from typing import Protocol

from data_preparation.domain.models.preparation.boundary import Boundary
from data_preparation.domain.models.preparation.model_metadata import ModelMetadata


class BoundariesCalculationService(Protocol):
    def calculate_percentile_boundaries(
            self,
            models: list[ModelMetadata],
            workers: int,
    ) -> list[Boundary]:
        ...
