from typing import Protocol

from domain.data_preparation.models.preparation.boundary import Boundary
from domain.data_preparation.models.preparation.model_metadata import ModelMetadata


class BoundariesCalculationService(Protocol):
    def calculate_percentile_boundaries(
            self,
            models: list[ModelMetadata],
            workers: int,
    ) -> list[Boundary]:
        ...
