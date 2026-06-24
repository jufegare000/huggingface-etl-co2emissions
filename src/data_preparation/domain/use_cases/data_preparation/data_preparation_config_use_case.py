from typing import Protocol, Any


class DataPreparationConfigUseCase(Protocol):

    def create_configuration_for_etl(self)-> dict[str, Any]:
        ...