import json
from typing import Any, Dict, List


def load_input_manifest(event: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "source_csv_path": event.get("source_csv_path", "s3://my-bucket/input/models.csv"),
        "workers": event.get("workers", 4),
        "threads_per_worker": event.get("threads_per_worker", 8),
    }


def validate_input(config: Dict[str, Any]) -> None:

    if config["workers"] <= 0:
        raise ValueError("workers must be > 0")

    if config["threads_per_worker"] <= 0:
        raise ValueError("threads_per_worker must be > 0")

    if not config["source_csv_path"]:
        raise ValueError("source_csv_path is required")


def load_models_metadata(config: Dict[str, Any]) -> List[Dict[str, Any]]:

    return [
        {"model_id": "org/model-a", "co2_eq_emissions": 10.5},
        {"model_id": "org/model-b", "co2_eq_emissions": 25.0},
        {"model_id": "org/model-c", "co2_eq_emissions": 40.2},
        {"model_id": "org/model-d", "co2_eq_emissions": 60.8},
    ]


def calculate_percentile_boundaries(
    models: List[Dict[str, Any]],
    workers: int,
) -> List[Dict[str, float]]:
    return [
        {"partition_id": i, "emission_min": i * 10.0, "emission_max": (i + 1) * 10.0}
        for i in range(workers)
    ]


def build_partition_descriptors(
    boundaries: List[Dict[str, float]],
    config: Dict[str, Any],
) -> List[Dict[str, Any]]:

    partitions = []

    for boundary in boundaries:
        partition_id = boundary["partition_id"]

        partitions.append({
            "partition_id": partition_id,
            "input_path": f"s3://my-bucket/prepared/partition_id={partition_id}/models.csv",
            "thread_count": config["threads_per_worker"],
            "emission_min": boundary["emission_min"],
            "emission_max": boundary["emission_max"],
            "status": "PENDING",
        })

    return partitions


def persist_preparation_output(partitions: List[Dict[str, Any]]) -> Dict[str, Any]:

    return {
        "manifest_path": "s3://my-bucket/prepared/manifest.json",
        "partitions_count": len(partitions),
    }


def build_step_function_output(
    partitions: List[Dict[str, Any]],
    persistence_result: Dict[str, Any],
) -> Dict[str, Any]:

    return {
        "manifest_path": persistence_result["manifest_path"],
        "partitions_count": persistence_result["partitions_count"],
        "partitions": partitions,
    }


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    config = load_input_manifest(event)
    validate_input(config)

    models = load_models_metadata(config)
    boundaries = calculate_percentile_boundaries(models, config["workers"])
    partitions = build_partition_descriptors(boundaries, config)
    persistence_result = persist_preparation_output(partitions)

    return build_step_function_output(partitions, persistence_result)