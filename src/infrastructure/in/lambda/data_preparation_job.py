import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from config.injection.dependency_injector import data_parsing_service
from config.injection.dependency_injector import s3_service
from config.injection.dependency_injector import lambda_config_service
from config.injection.dependency_injector import models_metadata_service
from config.injection.dependency_injector import boundaries_calculation_service
from config.injection.dependency_injector import partition_descriptor_service
from config.injection.dependency_injector import data_preparation_repository

import boto3

dynamodb = boto3.resource("dynamodb")

type FinalManifest = Dict[str, Any]
from typing import Any, TypedDict

from typing import Any, Dict, List, Tuple

def build_manifest(
    partitions: List[Dict[str, Any]],
    bucket: str,
    config: Dict[str, Any],
) -> Tuple[Dict[str, Any], str]:
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
        "created_at": data_parsing_service.utc_now_iso(),
        "partitions": partitions,
    }
    return manifest, manifest_key


def persist_preparation_output(
        partitions: List[Dict[str, Any]],
        bucket: str,
        config: Dict[str, Any],
) -> Dict[str, Any]:
    manifest, manifest_key = build_manifest(partitions, bucket, config)

    s3_service.write_json_to_s3(manifest, bucket, manifest_key)

    persistence_structure: Dict[str, Any] = data_preparation_repository.persist_preparation_output(partitions, bucket,
                                                                                                   config, manifest,
                                                                                                   manifest_key)

    return persistence_structure


def build_step_function_output(
        partitions: List[Dict[str, Any]],
        persistence_result: Dict[str, Any],
        bucket_name: str,
        config: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "run_id": config["run_id"],
        "bucket_name": bucket_name,
        "control_table_name": config["control_table_name"],
        "source_csv_path": config["source_csv_path"],
        "manifest_path": persistence_result["manifest_path"],
        "partitions_count": persistence_result["partitions_count"],
        "partitions": partitions,
    }


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    manifest = lambda_config_service.load_input_manifest(event)

    ai_models_metadata = models_metadata_service.load_models_metadata(manifest)

    boundaries = boundaries_calculation_service.calculate_percentile_boundaries(ai_models_metadata, manifest["workers"])

    partitions_descriptor = partition_descriptor_service.build_partition_descriptors(
        boundaries,
        manifest,
        ai_models_metadata,
    )

    persistence_result = persist_preparation_output(
        partitions_descriptor,
        manifest["bucket_name"],
        manifest,
    )

    return build_step_function_output(
        partitions_descriptor,
        persistence_result,
        manifest["bucket_name"],
        manifest,
    )
