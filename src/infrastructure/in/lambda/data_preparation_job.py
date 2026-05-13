import csv
import io
import json
import math
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import boto3


s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


DISCOVERY_DEFAULT_KEY = "discovery/hf-carbon/latest/models_with_emissions.csv"
PREPARED_PREFIX = "prepared/hf-carbon"

GLOBAL_RATE_LIMIT = 1000
WINDOW_SECONDS = 300
CALLS_PER_MODEL = 1

CSV_COLUMNS = [
    "model_id",
    "co2_eq_emissions",
    "co2_source",
    "training_type",
    "geographical_location",
    "hardware_used",
    "created_at",
    "downloads",
    "likes",
    "library_name",
    "pipeline_tag",
    "tags",
    "snapshot_id",
    "discovered_at",
]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def to_decimal(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: to_decimal(v) for k, v in value.items()}
    if isinstance(value, list):
        return [to_decimal(v) for v in value]
    return value


def parse_s3_uri(uri: str) -> Dict[str, str]:
    parsed = urlparse(uri)

    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3 URI, got: {uri}")

    return {
        "bucket": parsed.netloc,
        "key": parsed.path.lstrip("/"),
    }


def read_csv_from_s3(s3_uri: str) -> List[Dict[str, Any]]:
    parsed = parse_s3_uri(s3_uri)

    response = s3.get_object(
        Bucket=parsed["bucket"],
        Key=parsed["key"],
    )

    text = response["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    if not reader.fieldnames:
        raise ValueError(f"CSV has no header: {s3_uri}")

    if "model_id" not in reader.fieldnames:
        raise ValueError("CSV must contain model_id")

    if "co2_eq_emissions" not in reader.fieldnames:
        raise ValueError("CSV must contain co2_eq_emissions")

    return list(reader)


def write_csv_to_s3(rows: List[Dict[str, Any]], bucket: str, key: str) -> None:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )


def write_json_to_s3(payload: Dict[str, Any], bucket: str, key: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
        ContentType="application/json",
    )


def safe_float(value: Any) -> Optional[float]:
    if value is None:
        return None

    try:
        return float(str(value).strip())
    except ValueError:
        return None


def safe_int(value: Any) -> int:
    if value is None:
        return 0

    try:
        return int(float(str(value).strip()))
    except ValueError:
        return 0


def load_input_manifest(event: Dict[str, Any]) -> Dict[str, Any]:
    bucket_name = os.environ.get("RAW_BUCKET_NAME", "my-default-bucket")
    table_name = os.environ.get("CONTROL_TABLE_NAME")

    if not table_name:
        raise ValueError("CONTROL_TABLE_NAME environment variable is required")

    run_id = event.get("run_id", utc_now_compact())

    return {
        "run_id": run_id,
        "source_csv_path": event.get(
            "source_csv_path",
            f"s3://{bucket_name}/{DISCOVERY_DEFAULT_KEY}",
        ),
        "workers": int(event.get("workers", 4)),
        "threads_per_worker": int(event.get("threads_per_worker", 1)),
        "bucket_name": bucket_name,
        "control_table_name": table_name,
        "prepared_prefix": event.get(
            "prepared_prefix",
            f"{PREPARED_PREFIX}/run_id={run_id}",
        ),
        "global_rate_limit": int(event.get("global_rate_limit", GLOBAL_RATE_LIMIT)),
        "window_seconds": int(event.get("window_seconds", WINDOW_SECONDS)),
        "calls_per_model": int(event.get("calls_per_model", CALLS_PER_MODEL)),
    }


def validate_input(config: Dict[str, Any]) -> None:
    if config["workers"] <= 0:
        raise ValueError("workers must be > 0")

    if config["threads_per_worker"] <= 0:
        raise ValueError("threads_per_worker must be > 0")

    if config["global_rate_limit"] <= 0:
        raise ValueError("global_rate_limit must be > 0")

    if config["window_seconds"] <= 0:
        raise ValueError("window_seconds must be > 0")

    if config["calls_per_model"] <= 0:
        raise ValueError("calls_per_model must be > 0")

    if not config["source_csv_path"]:
        raise ValueError("source_csv_path is required")

    parse_s3_uri(config["source_csv_path"])


def load_models_metadata(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = read_csv_from_s3(config["source_csv_path"])

    models_by_id: Dict[str, Dict[str, Any]] = {}

    for row in rows:
        model_id = str(row.get("model_id") or "").strip()
        emissions = safe_float(row.get("co2_eq_emissions"))

        if not model_id:
            continue

        if emissions is None:
            continue

        row["model_id"] = model_id
        row["co2_eq_emissions"] = emissions
        row["downloads"] = safe_int(row.get("downloads"))
        row["likes"] = safe_int(row.get("likes"))

        models_by_id[model_id] = row

    models = list(models_by_id.values())

    models.sort(
        key=lambda item: (
            item.get("downloads", 0),
            item.get("likes", 0),
            item.get("co2_eq_emissions", 0),
        ),
        reverse=True,
    )

    if not models:
        raise ValueError("No valid models found in source CSV")

    return models


def calculate_percentile_boundaries(
        models: List[Dict[str, Any]],
        workers: int,
) -> List[Dict[str, Any]]:
    partition_size = max(
        1,
        math.floor(GLOBAL_RATE_LIMIT / workers / CALLS_PER_MODEL),
    )

    partitions_count = math.ceil(len(models) / partition_size)

    boundaries = []

    for i in range(partitions_count):
        start_index = i * partition_size
        end_index = min(start_index + partition_size, len(models))

        partition_models = models[start_index:end_index]

        emissions = [
            float(model["co2_eq_emissions"])
            for model in partition_models
            if model.get("co2_eq_emissions") is not None
        ]

        boundaries.append({
            "partition_id": i,
            "start_index": start_index,
            "end_index": end_index,
            "records_count": len(partition_models),
            "emission_min": min(emissions) if emissions else None,
            "emission_max": max(emissions) if emissions else None,
        })

    return boundaries


def build_partition_descriptors(
        boundaries: List[Dict[str, Any]],
        config: Dict[str, Any],
        models: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    partitions = []
    bucket = config["bucket_name"]
    prepared_prefix = config["prepared_prefix"].strip("/")

    for boundary in boundaries:
        partition_id = boundary["partition_id"]
        partition_id_str = f"{partition_id:06d}"

        start_index = boundary["start_index"]
        end_index = boundary["end_index"]

        partition_rows = models[start_index:end_index]

        input_key = f"{prepared_prefix}/partition_id={partition_id_str}/models.csv"

        write_csv_to_s3(
            rows=partition_rows,
            bucket=bucket,
            key=input_key,
        )

        partitions.append({
            "partition_id": partition_id_str,
            "input_path": f"s3://{bucket}/{input_key}",
            "thread_count": config["threads_per_worker"],
            "records_count": boundary["records_count"],
            "emission_min": boundary["emission_min"],
            "emission_max": boundary["emission_max"],
            "status": "PENDING",
        })

    return partitions


def persist_preparation_output(
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
        "created_at": utc_now_iso(),
        "partitions": partitions,
    }

    write_json_to_s3(manifest, bucket, manifest_key)

    table = dynamodb.Table(config["control_table_name"])

    table.put_item(
        Item=to_decimal({
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
            "created_at": utc_now_iso(),
            "updated_at": utc_now_iso(),
        })
    )

    table.put_item(
        Item=to_decimal({
            "PK": "PIPELINE#hf-carbon",
            "SK": "LAST_RUN",
            "entity_type": "LAST_RUN_POINTER",
            "run_id": config["run_id"],
            "status": "PREPARED",
            "source_csv_path": config["source_csv_path"],
            "manifest_path": manifest["manifest_path"],
            "partitions_count": len(partitions),
            "updated_at": utc_now_iso(),
        })
    )

    with table.batch_writer() as batch:
        for partition in partitions:
            now = utc_now_iso()

            batch.put_item(
                Item=to_decimal({
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
    config = load_input_manifest(event)
    validate_input(config)

    models = load_models_metadata(config)
    boundaries = calculate_percentile_boundaries(models, config["workers"])

    partitions = build_partition_descriptors(
        boundaries,
        config,
        models,
    )

    persistence_result = persist_preparation_output(
        partitions,
        config["bucket_name"],
        config,
    )

    return build_step_function_output(
        partitions,
        persistence_result,
        config["bucket_name"],
        config,
    )