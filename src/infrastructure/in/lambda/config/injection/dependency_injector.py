from application.data_preparation.services.config.boundaries.boundaries_calculation_service_implemented import \
    BoundariesCalculationServiceImplemented
from application.data_preparation.services.config.lambda_function.lambda_config_service_implemented import \
    LambdaConfigServiceImplemented
from application.data_preparation.services.config.lambda_function.lambda_config_validator_service_implemented import \
    LambdaConfigValidatorServiceImplemented
from application.data_preparation.services.config.manifests.data_manifest_builder_service_implemented import \
    DataManifestBuilderServiceImplemented

from application.data_preparation.services.config.partitions.partition_descriptor_service_implemented import \
    PartitionDescriptorServiceImplemented
from application.data_preparation.services.dates.date_parsing_service_implemented import \
    SystemDateTimeServiceImplemented
from domain.data_preparation.services.config.manifests.data_manifest_builder_service import DataManifestBuilderService
from infrastructure.out.dynamo.services.impl.dynamodb_conversion_service_implemented import \
    DynamoDBTypeConversionServiceImplemented
from application.data_preparation.services.metadata.ai_metadata_models.ai_models_metadata_parser_service_implemented import \
    AIModelsMetadataParserServiceImplemented
from application.data_preparation.services.metadata.ai_metadata_models.models_metadata_service_implemented import \
    AIAIModelsMetadataServiceImplemented
from application.data_preparation.services.s3.plain_text_reader_service_implemented import \
    PlainTextReaderServiceImplemented
from application.data_preparation.services.s3.s3_parser_service_implemented import S3ParserServiceImplemented

from application.data_preparation.services.s3.s3_service_implemented import S3ServiceImplemented

from domain.data_preparation.services.config.lambda_function.lambda_config_validator_service import \
    LambdaConfigValidatorService
from domain.data_preparation.services.config.boundaries.boundaries_calculation_service import \
    BoundariesCalculationService
from domain.data_preparation.services.config.partitions.partition_descriptor_service import PartitionDescriptorService
from domain.data_preparation.services.date_parsing_service import DataParsingService
from domain.data_preparation.services.metadata.ai_metadata_models.models_metadata_parser_service import \
    AIModelsMetadataParserService
from domain.data_preparation.services.metadata.ai_metadata_models.models_metadata_service import AIModelsMetadataService
from domain.data_preparation.services.plain_texts.plain_text_reader_service import PlainTextReaderService
from domain.data_preparation.services.s3.s3_parser_service import S3ParserService
from infrastructure.out.dynamo.mappers.dynamo_db_data_preparation_mapper import DynamoDBDataPreparationMapper
from infrastructure.out.dynamo.mappers.impl.dynamo_db_data_preparation_mapper_implemented import \
    DynamoDBDataPreparationMapperImplemented
from infrastructure.out.dynamo.services.type_conversion_service import TypeConversionService
from domain.data_preparation.services.s3.s3_service import S3Service
from domain.data_preparation.persistence.data_preparation_repository import DataPreparationRepository
from domain.data_preparation.services.config.lambda_function.lambda_config_service import LambdaConfigService
from infrastructure.out.dynamo.repository.dynamo_db_data_preparation_repository import DynamoDBDataPreparationRepository

data_parsing_service: DataParsingService = SystemDateTimeServiceImplemented()
type_conversion_service: TypeConversionService = DynamoDBTypeConversionServiceImplemented()
s3_uri_service: S3ParserService = S3ParserServiceImplemented()
s3_service: S3Service = S3ServiceImplemented()

lambda_config_validation_service: LambdaConfigValidatorService = LambdaConfigValidatorServiceImplemented(s3_uri_service)
lambda_config_service: LambdaConfigService = LambdaConfigServiceImplemented(data_parsing_service,
                                                                            lambda_config_validation_service)

plain_text_reader: PlainTextReaderService = PlainTextReaderServiceImplemented(s3_uri_service, s3_service)
ai_models_metadat_parser_service: AIModelsMetadataParserService = AIModelsMetadataParserServiceImplemented()
models_metadata_service: AIModelsMetadataService = AIAIModelsMetadataServiceImplemented(
    plain_text_reader,
    ai_models_metadat_parser_service
)
boundaries_calculation_service: BoundariesCalculationService = BoundariesCalculationServiceImplemented()
partition_descriptor_service: PartitionDescriptorService = PartitionDescriptorServiceImplemented(s3_service)
dynamo_db_data_preparation_mapper: DynamoDBDataPreparationMapper = DynamoDBDataPreparationMapperImplemented(
    data_parsing_service)
data_preparation_repository: DataPreparationRepository = DynamoDBDataPreparationRepository(
    dynamo_db_data_preparation_mapper, type_conversion_service)

data_manifest_builder: DataManifestBuilderService = DataManifestBuilderServiceImplemented(data_parsing_service)