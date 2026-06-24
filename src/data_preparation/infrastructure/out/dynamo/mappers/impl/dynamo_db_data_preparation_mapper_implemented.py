from data_preparation.domain.models.preparation.final_manifest import FinalManifest
from data_preparation.domain.models.preparation.input_manifest import InputManifest
from data_preparation.domain.models.preparation.partition_descriptor import PartitionDescriptor
from data_preparation.domain.services.date_parsing_service import DataParsingService
from data_preparation.infrastructure.out.dynamo.mappers.dynamo_db_data_preparation_mapper import DynamoDBDataPreparationMapper


class DynamoDBDataPreparationMapperImplemented(DynamoDBDataPreparationMapper):

    def __init__(self, data_parsing_service: DataParsingService):
        super().__init__(data_parsing_service)
        self.data_parsing_service = data_parsing_service

    def to_run_item(self, config: InputManifest, manifest: FinalManifest) -> dict:
        now = self.data_parsing_service.utc_now_iso()
        return {
            "PK": "PIPELINE#hf-carbon",
            "SK": f"RUN#{config.run_id}",
            "entity_type": "PREPARATION_RUN",
            "run_id": config.run_id,
            "status": "PREPARED",
            "source_csv_path": config.source_csv_path,
            "manifest_path": manifest.manifest_path,
            "partitions_count": len(manifest.partitions),
            "workers": config.workers,
            "threads_per_worker": config.threads_per_worker,
            "global_rate_limit": config.global_rate_limit,
            "window_seconds": config.window_seconds,
            "calls_per_model": config.calls_per_model,
            "created_at": now,
            "updated_at": now,
        }

    def to_last_run_item(self, config: InputManifest, manifest: FinalManifest) -> dict:
        return {
            "PK": "PIPELINE#hf-carbon",
            "SK": "LAST_RUN",
            "entity_type": "LAST_RUN_POINTER",
            "run_id": config.run_id,
            "status": "PREPARED",
            "source_csv_path": config.source_csv_path,
            "manifest_path": manifest.manifest_path,
            "partitions_count": len(manifest.partitions),
            "updated_at": self.data_parsing_service.utc_now_iso(),
        }

    def to_partition_item(self, config: InputManifest, partition: PartitionDescriptor) -> dict:
        now = self.data_parsing_service.utc_now_iso()
        return {
            "PK": f"RUN#{config.run_id}",
            "SK": f"PARTITION#{partition.partition_id}",

            "entity_type": "PARTITION",
            "run_id": config.run_id,
            "partition_id": partition.partition_id,

            "status": "PENDING",

            # Campos planos, mantenidos para compatibilidad
            "input_path": partition.input_path,
            "records_count": partition.records_count,
            "thread_count": partition.thread_count,
            "emission_min": partition.emission_min,
            "emission_max": partition.emission_max,

            # Estructura usada por el Glue enrichment job
            "input": {
                "uri": partition.input_path,
                "records_count": partition.records_count,
            },

            "output": {
                "prefix": f"enriched/hf-carbon/run_id={config.run_id}/partition_id={partition.partition_id}/",
                "results_path": None,
                "errors_path": None,
                "metrics_path": None,
                "success_marker_path": None,
            },

            "rate_budget": {
                "partition_call_budget": partition.records_count * config.calls_per_model,
                "estimated_calls": partition.records_count * config.calls_per_model,
                "global_rate_limit": config.global_rate_limit,
                "window_seconds": config.window_seconds,
                "calls_per_model": config.calls_per_model,
            },

            "execution": {
                "attempts": 0,
                "started_at": None,
                "completed_at": None,
                "glue_job_name": None,
                "glue_job_run_id": None,
            },

            "metrics": {
                "input_count": partition.records_count,
                "processed_count": 0,
                "success_count": 0,
                "failed_count": 0,
                "skipped_count": 0,
                "api_calls_count": 0,
                "batches_written": 0,
                "last_batch_path": None,
            },

            "error": {
                "last_error_type": None,
                "last_error_message": None,
            },

            "created_at": now,
            "updated_at": now,
        }