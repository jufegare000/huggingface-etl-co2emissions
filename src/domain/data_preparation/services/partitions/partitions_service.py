from typing import Protocol

from domain.data_preparation.models.preparation.step_functions_input import StepFunctionInput


class PartitionsService(Protocol):
    def build_partition_structure(
            self,
    ) -> StepFunctionInput:
        ...
