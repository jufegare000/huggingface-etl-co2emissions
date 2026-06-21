from application.services.data_preparation.config import \
    BoundariesCalculationServiceImplemented
from application.services.data_preparation.config.lambda_function.lambda_config_service_implemented import LambdaConfigServiceImplemented
from application.services.data_preparation.config.lambda_function import \
    LambdaConfigValidatorServiceImplemented
from application.services.data_preparation.config.partitions.partition_descriptor_service_implemented import \
    PartitionDescriptorServiceImplemented
from application.services.data_preparation.dates import SystemDateTimeServiceImplemented
from application.services.data_preparation.dynamodb.dynamodb_conversion_service_implemented import \
    DynamoDBTypeConversionServiceImplemented
from application.services.data_preparation.metadata import \
    AIModelsMetadataParserServiceImplemented
from application.services.data_preparation.metadata import \
    ModelsMetadataServiceImplemented
from application.services.data_preparation.s3 import PlainTextReaderServiceImplemented
from application.services.data_preparation.s3 import S3ParserServiceImplemented
from application.services.data_preparation.s3.s3_service_implemented import S3ServiceImplemented

from domain.data_preparation.services.config.lambda_function.lambda_config_validator_service import LambdaConfigValidatorService
from domain.data_preparation.services.config.boundaries.boundaries_calculation_service import BoundariesCalculationService
from domain.data_preparation.services.config.partitions.partition_descriptor_service import PartitionDescriptorService
from domain.data_preparation.services.date_parsing_service import DataParsingService
from domain.data_preparation.services.metadata.ai_metadata_models.models_metadata_parser_service import \
    AIModelsMetadataParserService
from domain.data_preparation.services.metadata.ai_metadata_models.models_metadata_service import ModelsMetadataService
from domain.data_preparation.services.plain_texts.plain_text_reader_service import PlainTextReaderService
from domain.data_preparation.services.s3.s3_parser_service import S3ParserService
from infrastructure.out.dynamo.services.type_conversion_service import TypeConversionService
from domain.data_preparation.services.s3.s3_service import S3Service
from domain.data_preparation.services.config.lambda_function.lambda_config_service import LambdaConfigService

data_parsing_service: DataParsingService = SystemDateTimeServiceImplemented()
type_conversion_service: TypeConversionService = DynamoDBTypeConversionServiceImplemented()
s3_uri_service: S3ParserService = S3ParserServiceImplemented
s3_service: S3Service = S3ServiceImplemented

lambda_config_validation_service: LambdaConfigValidatorService = LambdaConfigValidatorServiceImplemented(s3_uri_service)

lambda_config_service: LambdaConfigService = LambdaConfigServiceImplemented(data_parsing_service,
                                                                            lambda_config_validation_service)

plain_text_reader: PlainTextReaderService = PlainTextReaderServiceImplemented(s3_uri_service, s3_service)
ai_models_metadat_parser_service: AIModelsMetadataParserService = AIModelsMetadataParserServiceImplemented
models_metadata_service: ModelsMetadataService = ModelsMetadataServiceImplemented(plain_text_reader,
                                                                                  ai_models_metadat_parser_service)

boundaries_calculation_service: BoundariesCalculationService = BoundariesCalculationServiceImplemented
partition_descriptor_service: PartitionDescriptorService = PartitionDescriptorServiceImplemented
