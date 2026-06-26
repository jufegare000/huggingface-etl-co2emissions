from raw_ingestion.application.services.enrichment.row_enrichment_service import RowEnrichmentService
from raw_ingestion.application.services.ingestion.raw_ingestion_service import RawIngestionService
from raw_ingestion.application.services.rate_limit.rate_limit_service import RateLimitService
from raw_ingestion.infrastructure.out.dynamodb.partition_repository_implemented import (
    PartitionRepositoryImplemented,
)
from raw_ingestion.infrastructure.out.hf.hf_model_fetcher_implemented import (
    HfModelFetcherImplemented,
)
from raw_ingestion.infrastructure.out.s3.ingestion_s3_writer_implemented import (
    IngestionS3WriterImplemented,
)
from shared.application.services.date_parsing.date_parsing_service_implemented import (
    SystemDateTimeServiceImplemented,
)
from shared.application.services.security.hugging_face_secret_service import (
    HuggingFaceSecretService,
)
from shared.infrastructure.out.secrets_manager.secret_manager_obtainer import (
    SecretsManagerObtainer,
)


def build_raw_ingestion_service(table_name: str) -> RawIngestionService:
    return RawIngestionService(
        hf_model_fetcher=HfModelFetcherImplemented(),
        partition_repository=PartitionRepositoryImplemented(table_name),
        ingestion_writer=IngestionS3WriterImplemented(),
        row_enrichment_service=RowEnrichmentService(),
        rate_limit_service=RateLimitService(),
        date_parsing_service=SystemDateTimeServiceImplemented(),
        secret_obtainer=HuggingFaceSecretService(SecretsManagerObtainer()),
    )
