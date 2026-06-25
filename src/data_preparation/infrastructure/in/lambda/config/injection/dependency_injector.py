from data_preparation.application.services.config.boundaries.boundaries_calculation_service_implemented import \
    BoundariesCalculationServiceImplemented
from data_preparation.application.services.config.lambda_function.lambda_config_service_implemented import \
    LambdaConfigServiceImplemented
from data_preparation.application.services.config.lambda_function.lambda_config_validator_service_implemented import \
    LambdaConfigValidatorServiceImplemented
from data_preparation.application.services.config.manifests.data_manifest_builder_service_implemented import \
    DataManifestBuilderServiceImplemented

from data_preparation.application.services.config.partitions.partition_descriptor_service_implemented import \
    PartitionDescriptorServiceImplemented
from shared.application.services.date_parsing.date_parsing_service_implemented import \
    SystemDateTimeServiceImplemented
from data_preparation.application.services.partitions.partitions_service_implemented import PartitionsServiceImplemented
from data_preparation.application.services.step_functions.step_functions_service_implemented import StepFunctionsServiceImplemented
from data_preparation.application.use_cases.data_preparation.data_preparation_config_use_case_implemented import \
    DataPreparationConfigUseCaseImplemented
from data_preparation.domain.services.config.manifests.data_manifest_builder_service import DataManifestBuilderService
from data_preparation.domain.services.step_functions.step_functions_service import StepFunctionsService
from data_preparation.domain.services.partitions.partitions_service import PartitionsService
from data_preparation.domain.use_cases.data_preparation.data_preparation_config_use_case import \
    DataPreparationConfigUseCase

from data_preparation.infrastructure.out.dynamo.services.impl.dynamodb_conversion_service_implemented import \
    DynamoDBTypeConversionServiceImplemented
from data_preparation.application.services.metadata.ai_metadata_models.ai_models_metadata_parser_service_implemented import \
    AIModelsMetadataParserServiceImplemented
from data_preparation.application.services.metadata.ai_metadata_models.models_metadata_service_implemented import \
    AIAIModelsMetadataServiceImplemented
from shared.application.services.s3.plain_text_reader_service_implemented import PlainTextReaderServiceImplemented
from shared.application.services.s3.s3_parser_service_implemented import S3ParserServiceImplemented
from shared.application.services.s3.s3_reader_service_implemented import S3ReaderServiceImplemented

from shared.application.services.s3.s3_writer_service_implemented import S3WriterServiceImplemented

from data_preparation.domain.services.config.lambda_function.lambda_config_validator_service import \
    LambdaConfigValidatorService
from data_preparation.domain.services.config.boundaries.boundaries_calculation_service import \
    BoundariesCalculationService
from data_preparation.domain.services.config.partitions.partition_descriptor_service import PartitionDescriptorService
from shared.domain.services.plain_texts.plain_text_reader_service import PlainTextReaderService
from shared.domain.services.date_parsing.date_parsing_service import DataParsingService
from data_preparation.domain.services.metadata.ai_metadata_models.models_metadata_parser_service import \
    AIModelsMetadataParserService
from data_preparation.domain.services.metadata.ai_metadata_models.models_metadata_service import AIModelsMetadataService
from data_preparation.domain.services.s3.s3_parser_service import S3ParserService
from data_preparation.infrastructure.out.dynamo.mappers.dynamo_db_data_preparation_mapper import DynamoDBDataPreparationMapper
from data_preparation.infrastructure.out.dynamo.mappers.impl.dynamo_db_data_preparation_mapper_implemented import \
    DynamoDBDataPreparationMapperImplemented
from data_preparation.infrastructure.out.dynamo.services.type_conversion_service import TypeConversionService
from data_preparation.domain.persistence.data_preparation_repository import DataPreparationRepository
from data_preparation.domain.services.config.lambda_function.lambda_config_service import LambdaConfigService
from data_preparation.infrastructure.out.dynamo.repository.dynamo_db_data_preparation_repository import DynamoDBDataPreparationRepository
from shared.domain.services.s3.s3_writer_service import S3WriterService

data_parsing_service: DataParsingService = SystemDateTimeServiceImplemented()
type_conversion_service: TypeConversionService = DynamoDBTypeConversionServiceImplemented()
s3_uri_service: S3ParserService = S3ParserServiceImplemented()
s3_writer_service: S3WriterService = S3WriterServiceImplemented()
s3_reader_service: S3WriterService = S3ReaderServiceImplemented()

lambda_config_validation_service: LambdaConfigValidatorService = LambdaConfigValidatorServiceImplemented(s3_uri_service)
lambda_config_service: LambdaConfigService = LambdaConfigServiceImplemented(data_parsing_service,
                                                                            lambda_config_validation_service)

plain_text_reader: PlainTextReaderService = PlainTextReaderServiceImplemented(s3_uri_service, s3_writer_service, s3_reader_service)
ai_models_metadat_parser_service: AIModelsMetadataParserService = AIModelsMetadataParserServiceImplemented()
models_metadata_service: AIModelsMetadataService = AIAIModelsMetadataServiceImplemented(
    plain_text_reader,
    ai_models_metadat_parser_service
)
boundaries_calculation_service: BoundariesCalculationService = BoundariesCalculationServiceImplemented()
partition_descriptor_service: PartitionDescriptorService = PartitionDescriptorServiceImplemented(s3_writer_service)
dynamo_db_data_preparation_mapper: DynamoDBDataPreparationMapper = DynamoDBDataPreparationMapperImplemented(
    data_parsing_service)
data_preparation_repository: DataPreparationRepository = DynamoDBDataPreparationRepository(
    dynamo_db_data_preparation_mapper, type_conversion_service)

data_manifest_builder: DataManifestBuilderService = DataManifestBuilderServiceImplemented(data_parsing_service)

step_functions_service: StepFunctionsService = StepFunctionsServiceImplemented()

partition_service: PartitionsService = PartitionsServiceImplemented(lambda_config_service,
                                                                    models_metadata_service,
                                                                    boundaries_calculation_service,
                                                                    partition_descriptor_service,
                                                                    data_manifest_builder,
                                                                    s3_writer_service,
                                                                    data_preparation_repository, )

data_preparation_config_use_case: DataPreparationConfigUseCase = DataPreparationConfigUseCaseImplemented(partition_service, step_functions_service)

