from domain.data_preparation.services.data_preparation.data_preparation_persistence import DataPreparationPersistence
from typing import Any, Dict, List

from domain.data_preparation.services.date_parsing_service import DataParsingService
from domain.data_preparation.services.s3.s3_service import S3Service

# TODO: This class is outdated and unused, It can be removed?
class DataPreparationPersistenceImplemented(DataPreparationPersistence):

    def __init__(self, data_parsing_service: DataParsingService, s3_service: S3Service):
        self.data_parsing_service = data_parsing_service
        self.s3_service = s3_service

    def persist_preparation_output(
            self,
            partitions: List[Dict[str, Any]],
            bucket: str,
            config: Dict[str, Any],
    ) -> Dict[str, Any]:
        manifest_key = f"{config['prepared_prefix'].strip('/')}/manifest.json"

        manifest = {
            "run_id": config["run_id"],
            "status": "PREPARED",
            "source_csv_path": config["source_csv_path"],
            "manifest_path": f"s3://{bucket}/{manifest_key}",
            "partitions_count": len(partitions),
            "workers": config["workers"],
            "threads_per_worker": config["threads_per_worker"],
            "global_rate_limit": config["global_rate_limit"],
            "window_seconds": config["window_seconds"],
            "calls_per_model": config["calls_per_model"],
            "created_at": self.data_parsing_service.utc_now_iso(),
            "partitions": partitions,
        }

        self.s3_service.write_json_to_s3(manifest, bucket, manifest_key)

        table = dynamodb.Table(config["control_table_name"])

        table.put_item(
            Item=type_conversion_service.convert_floats_to_decimal({
                "PK": "PIPELINE#hf-carbon",
                "SK": f"RUN#{config['run_id']}",
                "entity_type": "PREPARATION_RUN",
                "run_id": config["run_id"],
                "status": "PREPARED",
                "source_csv_path": config["source_csv_path"],
                "manifest_path": manifest["manifest_path"],
                "partitions_count": len(partitions),
                "workers": config["workers"],
                "threads_per_worker": config["threads_per_worker"],
                "global_rate_limit": config["global_rate_limit"],
                "window_seconds": config["window_seconds"],
                "calls_per_model": config["calls_per_model"],
                "created_at": data_parsing_service.utc_now_iso(),
                "updated_at": data_parsing_service.utc_now_iso(),
            })
        )

        table.put_item(
            Item=type_conversion_service.convert_floats_to_decimal({
                "PK": "PIPELINE#hf-carbon",
                "SK": "LAST_RUN",
                "entity_type": "LAST_RUN_POINTER",
                "run_id": config["run_id"],
                "status": "PREPARED",
                "source_csv_path": config["source_csv_path"],
                "manifest_path": manifest["manifest_path"],
                "partitions_count": len(partitions),
                "updated_at": data_parsing_service.utc_now_iso(),
            })
        )

        with table.batch_writer() as batch:
            for partition in partitions:
                now = data_parsing_service.utc_now_iso()

                batch.put_item(
                    Item=type_conversion_service.convert_floats_to_decimal({
                        "PK": f"RUN#{config['run_id']}",
                        "SK": f"PARTITION#{partition['partition_id']}",

                        "entity_type": "PARTITION",
                        "run_id": config["run_id"],
                        "partition_id": partition["partition_id"],

                        "status": "PENDING",

                        # Campos planos, mantenidos para compatibilidad
                        "input_path": partition["input_path"],
                        "records_count": partition["records_count"],
                        "thread_count": partition["thread_count"],
                        "emission_min": partition["emission_min"],
                        "emission_max": partition["emission_max"],

                        # Estructura usada por el Glue enrichment job
                        "input": {
                            "uri": partition["input_path"],
                            "records_count": partition["records_count"],
                        },

                        "output": {
                            "prefix": f"enriched/hf-carbon/run_id={config['run_id']}/partition_id={partition['partition_id']}/",
                            "results_path": None,
                            "errors_path": None,
                            "metrics_path": None,
                            "success_marker_path": None,
                        },

                        "rate_budget": {
                            "partition_call_budget": partition["records_count"] * config["calls_per_model"],
                            "estimated_calls": partition["records_count"] * config["calls_per_model"],
                            "global_rate_limit": config["global_rate_limit"],
                            "window_seconds": config["window_seconds"],
                            "calls_per_model": config["calls_per_model"],
                        },

                        "execution": {
                            "attempts": 0,
                            "started_at": None,
                            "completed_at": None,
                            "glue_job_name": None,
                            "glue_job_run_id": None,
                        },

                        "metrics": {
                            "input_count": partition["records_count"],
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
                    })
                )

        return {
            "manifest_path": manifest["manifest_path"],
            "partitions_count": len(partitions),
        }
