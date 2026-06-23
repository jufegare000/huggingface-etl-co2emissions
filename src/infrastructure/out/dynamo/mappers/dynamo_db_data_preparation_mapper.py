from typing import Protocol

from domain.data_preparation.models.preparation.final_manifest import FinalManifest
from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor
from domain.data_preparation.services.date_parsing_service import DataParsingService


class DynamoDBDataPreparationMapper(Protocol):

    def __init__(self, data_parsing_service: DataParsingService):
        self.data_parsing_service = data_parsing_service

    def to_run_item(self, config: InputManifest, manifest: FinalManifest) -> dict:
        ...

    def to_last_run_item(self, config: InputManifest, manifest: FinalManifest) -> dict:
        ...

    def to_partition_item(self, config: InputManifest, partition: PartitionDescriptor) -> dict:
        ...
