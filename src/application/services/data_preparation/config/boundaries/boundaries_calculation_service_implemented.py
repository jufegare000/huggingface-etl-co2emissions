from typing import Any, Dict, List
import math

from application.services.data_preparation.config.lambda_function.data_preparation_enum import DataPreparationConfigParams
from domain.data_preparation.services.config.boundaries.boundaries_calculation_service import BoundariesCalculationService

class BoundariesCalculationServiceImplemented(BoundariesCalculationService):
    def calculate_percentile_boundaries(
            self,
            models: List[Dict[str, Any]],
            workers: int,
    ) -> List[Dict[str, Any]]:
        partition_size = max(
            1,
            math.floor(
                DataPreparationConfigParams.GLOBAL_RATE_LIMIT.value / workers / DataPreparationConfigParams.CALLS_PER_MODEL.value),
        )

        partitions_count = math.ceil(len(models) / partition_size)

        boundaries = []

        for i in range(partitions_count):
            start_index = i * partition_size
            end_index = min(start_index + partition_size, len(models))

            partition_models = models[start_index:end_index]

            emissions = [
                float(model["co2_eq_emissions"])
                for model in partition_models
                if model.get("co2_eq_emissions") is not None
            ]

            boundaries.append({
                "partition_id": i,
                "start_index": start_index,
                "end_index": end_index,
                "records_count": len(partition_models),
                "emission_min": min(emissions) if emissions else None,
                "emission_max": max(emissions) if emissions else None,
            })

        return boundaries