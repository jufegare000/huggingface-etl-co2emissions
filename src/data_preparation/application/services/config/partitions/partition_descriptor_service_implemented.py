from data_preparation.domain.models.preparation.boundary import Boundary
from data_preparation.domain.models.preparation.input_manifest import InputManifest
from data_preparation.domain.models.preparation.model_metadata import ModelMetadata
from data_preparation.domain.models.preparation.partition_descriptor import PartitionDescriptor
from data_preparation.domain.models.preparation.partition_status import PartitionStatus
from data_preparation.domain.services.config.partitions.partition_descriptor_service import PartitionDescriptorService
from shared.domain.services.s3.s3_writer_service import S3WriterService


class PartitionDescriptorServiceImplemented(PartitionDescriptorService):

    def __init__(self, s3_service: S3WriterService):
        self.s3_service = s3_service

    def build_partition_descriptors(
            self,
            boundaries: list[Boundary],
            config: InputManifest,
            models: list[ModelMetadata],
    ) -> list[PartitionDescriptor]:
        partitions: list[PartitionDescriptor] = []
        bucket = config.bucket_name
        prepared_prefix = config.prepared_prefix.strip("/")

        for boundary in boundaries:
            partition_id = boundary.partition_id
            partition_id_str = f"{partition_id:06d}"

            start_index = boundary.start_index
            end_index = boundary.end_index

            partition_rows = models[start_index:end_index]

            input_key = f"{prepared_prefix}/partition_id={partition_id_str}/ai_metadata_models.csv"

            self.s3_service.write_csv_to_s3(
                rows=partition_rows,
                bucket=bucket,
                key=input_key,
            )

            partitions.append(PartitionDescriptor(
                partition_id=partition_id_str,
                input_path=f"s3://{bucket}/{input_key}",
                thread_count=config.threads_per_worker,
                records_count=boundary.records_count,
                emission_min=boundary.emission_min,
                emission_max=boundary.emission_max,
                status=PartitionStatus.PENDING,
            ))
        return partitions
