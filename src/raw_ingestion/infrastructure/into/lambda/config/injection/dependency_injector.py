from raw_ingestion.application.services.error_redistribution.error_redistribution_service import (
    ErrorRedistributionService,
)
from raw_ingestion.infrastructure.out.dynamodb.partition_repository_implemented import (
    PartitionRepositoryImplemented,
)
from raw_ingestion.infrastructure.out.s3.ingestion_s3_writer_implemented import (
    IngestionS3WriterImplemented,
)


def build_error_redistribution_service(table_name: str) -> ErrorRedistributionService:
    return ErrorRedistributionService(
        ingestion_writer=IngestionS3WriterImplemented(),
        partition_repository=PartitionRepositoryImplemented(table_name),
    )
