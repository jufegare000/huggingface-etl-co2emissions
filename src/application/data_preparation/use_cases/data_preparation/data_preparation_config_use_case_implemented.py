from typing import Any

from domain.data_preparation.services.partitions.partitions_service import PartitionsService
from domain.data_preparation.services.step_functions.step_functions_service import StepFunctionsService
from domain.data_preparation.use_cases.data_preparation.data_preparation_config_use_case import \
    DataPreparationConfigUseCase


class DataPreparationConfigUseCaseImplemented(DataPreparationConfigUseCase):

    def __init__(self, partition_service: PartitionsService, step_functions_service: StepFunctionsService) -> None:
        self.partition_service = partition_service
        self.step_functions_service = step_functions_service

    def create_configuration_for_etl(self)-> dict[str, Any]:
        step_functions_input = self.partition_service.build_partition_structure()

        return self.step_functions_service.build_step_function_output(
            step_functions_input
        ).to_dict()
