from data_preparation.domain.models.preparation.step_function_output import StepFunctionOutput
from data_preparation.domain.models.preparation.step_functions_input import StepFunctionInput
from data_preparation.domain.services.step_functions.step_functions_service import StepFunctionsService


class StepFunctionsServiceImplemented(StepFunctionsService):

    def build_step_function_output(
            self,
            step_functions_input: StepFunctionInput,
    ) -> StepFunctionOutput:
        return StepFunctionOutput(
            run_id=step_functions_input.input_manifest.run_id,
            bucket_name=step_functions_input.bucket_name,
            control_table_name=step_functions_input.input_manifest.control_table_name,
            source_csv_path=step_functions_input.input_manifest.source_csv_path,
            manifest_path=step_functions_input.persistence_result.manifest_path,
            partitions_count=step_functions_input.persistence_result.partitions_count,
            partitions=step_functions_input.partitions,
        )
