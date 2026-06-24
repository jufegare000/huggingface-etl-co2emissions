from typing import Protocol

from data_preparation.domain.models.preparation.step_functions_input import StepFunctionInput


class PartitionsService(Protocol):
    def build_partition_structure(
            self,
    ) -> StepFunctionInput:
        ...
