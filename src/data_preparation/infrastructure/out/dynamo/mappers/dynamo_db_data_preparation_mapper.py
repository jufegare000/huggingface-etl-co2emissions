from typing import Protocol

from data_preparation.domain.models.preparation.final_manifest import FinalManifest
from data_preparation.domain.models.preparation.input_manifest import InputManifest
from data_preparation.domain.models.preparation.partition_descriptor import PartitionDescriptor
from shared.domain.services.date_parsing.date_parsing_service import DataParsingService


class DynamoDBDataPreparationMapper(Protocol):

    def __init__(self, data_parsing_service: DataParsingService):
        self.data_parsing_service = data_parsing_service

    def to_run_item(self, config: InputManifest, manifest: FinalManifest) -> dict:
        ...

    def to_last_run_item(self, config: InputManifest, manifest: FinalManifest) -> dict:
        ...

    def to_partition_item(self, config: InputManifest, partition: PartitionDescriptor) -> dict:
        ...
