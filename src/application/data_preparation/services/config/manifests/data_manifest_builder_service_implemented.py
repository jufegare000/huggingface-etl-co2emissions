from domain.data_preparation.models.preparation.final_manifest import FinalManifest
from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.models.preparation.manifest_status import ManifestStatus
from domain.data_preparation.models.preparation.partition_descriptor import PartitionDescriptor
from domain.data_preparation.services.config.manifests.data_manifest_builder_service import DataManifestBuilderService
from domain.data_preparation.services.date_parsing_service import DataParsingService


class DataManifestBuilderServiceImplemented(DataManifestBuilderService):

    def __init__(self, data_parsing_service: DataParsingService):
        self.data_parsing_service = data_parsing_service

    def build_manifest(self,
                       partitions: list[PartitionDescriptor],
                       bucket: str,
                       config: InputManifest,
                       ) -> tuple[FinalManifest, str]:
        manifest_key = f"{config.prepared_prefix.strip('/')}/manifest.json"
        manifest = FinalManifest(
            run_id=config.run_id,
            status=ManifestStatus.PREPARED,
            source_csv_path=config.source_csv_path,
            manifest_path=f"s3://{bucket}/{manifest_key}",
            partitions_count=len(partitions),
            workers=config.workers,
            threads_per_worker=config.threads_per_worker,
            global_rate_limit=config.global_rate_limit,
            window_seconds=config.window_seconds,
            calls_per_model=config.calls_per_model,
            created_at=self.data_parsing_service.utc_now_iso(),
            partitions=partitions,
        )
        return manifest, manifest_key
