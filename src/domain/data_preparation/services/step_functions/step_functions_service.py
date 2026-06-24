from typing import Protocol

from domain.data_preparation.models.preparation.step_function_output import StepFunctionOutput
from domain.data_preparation.models.preparation.step_functions_input import StepFunctionInput

class StepFunctionsService(Protocol):

    def build_step_function_output(
            self,
            step_functions_input: StepFunctionInput,
    ) -> StepFunctionOutput:
        ...
