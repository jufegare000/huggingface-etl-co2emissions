from data_discovery.application.services.checkpoint.checkpoint_service_implemented import CheckPointServiceImplemented
from data_discovery.application.services.consolidation.snapshot_consolidation_service_implemented import SnapshotConsolidationServiceImplemented
from data_discovery.application.services.discovery.discovery_event_logger_implemented import DiscoveryEventLoggerImplemented
from data_discovery.application.services.discovery.discovery_service import DiscoveryJobService
from data_discovery.application.services.discovery.part_flusher_service_implemented import PartFlusherServiceImplemented
from data_discovery.application.services.discovery.rate_limit_handler_service_implemented import RateLimitHandlerServiceImplemented
from data_discovery.infrastructure.into.glue.discovery.config.discovery_config import TARGET_BUCKET
from data_discovery.infrastructure.out.hugging_face.hf_models_api_client_implemented import HfModelsApiClientImplemented
from shared.application.services.date_parsing.date_parsing_service_implemented import SystemDateTimeServiceImplemented
from shared.application.services.hugging_face.hugging_face_types_helper_implemented import HuggingFaceTypesHelperImplemented
from shared.application.services.s3.s3_reader_service_implemented import S3ReaderServiceImplemented
from shared.application.services.s3.s3_writer_service_implemented import S3WriterServiceImplemented
from shared.application.services.security.hugging_face_secret_service import HuggingFaceSecretService
from shared.application.services.uploader_process.uploader_process_service_implemented import UploaderProcessServiceImplemented
from shared.domain.services.date_parsing.date_parsing_service import DataParsingService
from shared.domain.services.hugging_face.hugging_face_type_helpers import HuggingFaceTypesHelpers
from shared.domain.services.s3.s3_json_reader_service import S3JsonReaderService
from shared.domain.services.s3.s3_reader_service import S3ReaderService
from shared.domain.services.s3.s3_writer_service import S3WriterService
from shared.domain.services.security.secret_obtainer import SecretObtainer
from shared.domain.services.uploader_process.uploader_process_service import UploaderProcessService
from shared.infrastructure.out.s3.services.s3_json_reader_service_implemented import S3JsonReaderServiceImplemented
from shared.infrastructure.out.secrets_manager.secret_manager_obtainer import SecretsManagerObtainer

date_parsing_service: DataParsingService = SystemDateTimeServiceImplemented()

secret_obtainer: SecretObtainer = SecretsManagerObtainer()
hugging_face_secret_obtainer: SecretObtainer = HuggingFaceSecretService(secret_obtainer)

s3_reader_service: S3ReaderService = S3ReaderServiceImplemented()
s3_writer_service: S3WriterService = S3WriterServiceImplemented()
s3_json_reader_service: S3JsonReaderService = S3JsonReaderServiceImplemented(s3_reader_service)

hugging_face_type_helpers: HuggingFaceTypesHelpers = HuggingFaceTypesHelperImplemented()
uploader_process_service: UploaderProcessService = UploaderProcessServiceImplemented(s3_writer_service, TARGET_BUCKET)

hf_api_client = HfModelsApiClientImplemented()

checkpoint_service = CheckPointServiceImplemented(
    s3_json_reader_service=s3_json_reader_service,
    s3_writer_service=s3_writer_service,
    date_parsing_service=date_parsing_service,
    target_bucket=TARGET_BUCKET,
)

consolidation_service = SnapshotConsolidationServiceImplemented(
    s3_reader_service=s3_reader_service,
    s3_writer_service=s3_writer_service,
    uploader_process_service=uploader_process_service,
    target_bucket=TARGET_BUCKET,
)

event_logger = DiscoveryEventLoggerImplemented()

part_flusher = PartFlusherServiceImplemented(
    uploader_process_service=uploader_process_service,
    checkpoint_service=checkpoint_service,
    event_logger=event_logger,
)

rate_limit_handler = RateLimitHandlerServiceImplemented(
    hf_api_client=hf_api_client,
    checkpoint_service=checkpoint_service,
    part_flusher=part_flusher,
    event_logger=event_logger,
    date_parsing_service=date_parsing_service,
)

discovery_job_service: DiscoveryJobService = DiscoveryJobService(
    date_parsing_service=date_parsing_service,
    secrets_obtainer=hugging_face_secret_obtainer,
    hugging_face_types_helpers=hugging_face_type_helpers,
    hf_api_client=hf_api_client,
    checkpoint_service=checkpoint_service,
    consolidation_service=consolidation_service,
    part_flusher=part_flusher,
    rate_limit_handler=rate_limit_handler,
    event_logger=event_logger,
    target_bucket=TARGET_BUCKET,
)
