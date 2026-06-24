from data_preparation.domain.models.preparation.final_manifest import FinalManifest
from data_preparation.domain.models.preparation.input_manifest import InputManifest
from data_preparation.domain.models.preparation.persistence_structure import PersistenceStructure
from data_preparation.domain.persistence.data_preparation_repository import DataPreparationRepository
from data_preparation.infrastructure.out.dynamo.mappers.dynamo_db_data_preparation_mapper import DynamoDBDataPreparationMapper
from data_preparation.infrastructure.out.dynamo.services.type_conversion_service import TypeConversionService
from data_preparation.infrastructure.out.dynamo.dynamo_db_client import DynamoDBClient


class DynamoDBDataPreparationRepository(DataPreparationRepository):

    def __init__(self, dynamo_data_preparation_mapper: DynamoDBDataPreparationMapper,
                 type_conversion_service: TypeConversionService,
                 ):
        self.dynamo_data_mapper = dynamo_data_preparation_mapper
        self.type_conversion_service = type_conversion_service

    def persist_preparation_output(
            self,
            bucket: str,
            config: InputManifest,
            manifest: FinalManifest,
            manifest_key: str
    ) -> PersistenceStructure:
        table = DynamoDBClient.dynamodb_client.Table(config.control_table_name)

        table.put_item(Item=self.type_conversion_service.convert_floats_to_decimal(
            self.dynamo_data_mapper.to_run_item(config, manifest)
        ))

        table.put_item(Item=self.type_conversion_service.convert_floats_to_decimal(
            self.dynamo_data_mapper.to_last_run_item(config, manifest)
        ))

        with table.batch_writer() as batch:
            for partition in manifest.partitions:
                batch.put_item(Item=self.type_conversion_service.convert_floats_to_decimal(
                    self.dynamo_data_mapper.to_partition_item(config, partition)
                ))

        return PersistenceStructure(
            manifest_path=manifest.manifest_path,
            partitions_count=len(manifest.partitions),
        )