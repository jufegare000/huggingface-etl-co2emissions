import sys
import boto3
import json
import csv
import io
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "hf_token_secret_name",
    "run_id",
    "partition_id",
    "control_table_name"
])


HF_MODEL_API_BASE_URL = "https://huggingface.co/api/models"
DEFAULT_BATCH_SIZE = 50
DEFAULT_WINDOW_SECONDS = 300
DEFAULT_REQUEST_TIMEOUT_SECONDS = 60


s3 = boto3.client("s3")
secretsmanager = boto3.client("secretsmanager")
dynamodb = boto3.resource("dynamodb")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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
        raise ValueError(f"Expected S3 URI, got: {uri}")

    return {
        "bucket": parsed.netloc,
        "key": parsed.path.lstrip("/")
    }


def get_hf_token(secret_name: str) -> str:
    try:
        response = secretsmanager.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        print(f"Error recuperando el secreto: {exc}")
        raise

    secret_string = response.get("SecretString")

    if not secret_string:
        raise ValueError(f"Secret {secret_name} does not contain SecretString")

    stripped = secret_string.strip()

    if not stripped.startswith("{"):
        return stripped

    payload = json.loads(stripped)

    for key in ("HF_TOKEN", "hf_token", "token"):
        if payload.get(key):
            return str(payload[key]).strip()

    raise ValueError(
        f"Secret {secret_name} is JSON but does not contain HF_TOKEN, hf_token or token"
    )


def normalize_partition_id(value: str) -> str:
    text = str(value)

    if text.isdigit():
        return f"{int(text):06d}"

    return text


def get_partition_config(
    table_name: str,
    run_id: str,
    partition_id: str
) -> Dict[str, Any]:
    table = dynamodb.Table(table_name)

    response = table.get_item(
        Key={
            "PK": f"RUN#{run_id}",
            "SK": f"PARTITION#{partition_id}"
        }
    )

    item = response.get("Item")

    if not item:
        raise ValueError(
            f"Partition config not found in DynamoDB: run_id={run_id}, partition_id={partition_id}"
        )

    return item


def mark_partition_running(
    table_name: str,
    run_id: str,
    partition_id: str,
    job_name: str
) -> None:
    table = dynamodb.Table(table_name)

    table.update_item(
        Key={
            "PK": f"RUN#{run_id}",
            "SK": f"PARTITION#{partition_id}"
        },
        UpdateExpression="""
            SET #status = :running,
                updated_at = :updated_at,
                started_at = if_not_exists(started_at, :started_at),
                glue_job_name = :job_name
            ADD attempts :one
        """,
        ConditionExpression="#status = :pending OR #status = :failed",
        ExpressionAttributeNames={
            "#status": "status"
        },
        ExpressionAttributeValues=to_decimal({
            ":running": "RUNNING",
            ":pending": "PENDING",
            ":failed": "FAILED",
            ":updated_at": utc_now_iso(),
            ":started_at": utc_now_iso(),
            ":job_name": job_name,
            ":one": 1
        })
    )


def update_partition_progress(
    table_name: str,
    run_id: str,
    partition_id: str,
    processed_count: int,
    success_count: int,
    failed_count: int,
    api_calls_count: int,
    batches_written: int,
    last_batch_path: Optional[str]
) -> None:
    table = dynamodb.Table(table_name)

    table.update_item(
        Key={
            "PK": f"RUN#{run_id}",
            "SK": f"PARTITION#{partition_id}"
        },
        UpdateExpression="""
            SET processed_count = :processed_count,
                success_count = :success_count,
                failed_count = :failed_count,
                api_calls_count = :api_calls_count,
                batches_written = :batches_written,
                last_batch_path = :last_batch_path,
                updated_at = :updated_at
        """,
        ExpressionAttributeValues=to_decimal({
            ":processed_count": processed_count,
            ":success_count": success_count,
            ":failed_count": failed_count,
            ":api_calls_count": api_calls_count,
            ":batches_written": batches_written,
            ":last_batch_path": last_batch_path,
            ":updated_at": utc_now_iso()
        })
    )

def mark_partition_completed(
    table_name: str,
    run_id: str,
    partition_id: str,
    output_prefix: str,
    success_marker_path: str,
    input_count: int,
    success_count: int,
    failed_count: int,
    api_calls_count: int,
    batches_written: int
) -> None:
    table = dynamodb.Table(table_name)

    table.update_item(
        Key={
            "PK": f"RUN#{run_id}",
            "SK": f"PARTITION#{partition_id}"
        },
        UpdateExpression="""
            SET #status = :completed,
                output_prefix = :output_prefix,
                success_marker_path = :success_marker_path,
                metrics.input_count = :input_count,
                metrics.processed_count = :processed_count,
                metrics.success_count = :success_count,
                metrics.failed_count = :failed_count,
                metrics.api_calls_count = :api_calls_count,
                metrics.batches_written = :batches_written,
                updated_at = :updated_at,
                completed_at = :completed_at
        """,
        ExpressionAttributeNames={
            "#status": "status"
        },
        ExpressionAttributeValues=to_decimal({
            ":completed": "COMPLETED",
            ":output_prefix": output_prefix,
            ":success_marker_path": success_marker_path,
            ":input_count": input_count,
            ":processed_count": success_count + failed_count,
            ":success_count": success_count,
            ":failed_count": failed_count,
            ":api_calls_count": api_calls_count,
            ":batches_written": batches_written,
            ":updated_at": utc_now_iso(),
            ":completed_at": utc_now_iso()
        })
    )


def mark_partition_failed(
    table_name: str,
    run_id: str,
    partition_id: str,
    error: Exception,
    processed_count: int,
    success_count: int,
    failed_count: int,
    api_calls_count: int
) -> None:
    table = dynamodb.Table(table_name)

    try:
        table.update_item(
            Key={
                "PK": f"RUN#{run_id}",
                "SK": f"PARTITION#{partition_id}"
            },
            UpdateExpression="""
                SET #status = :failed,
                    processed_count = :processed_count,
                    success_count = :success_count,
                    failed_count = :failed_count,
                    api_calls_count = :api_calls_count,
                    last_error_type = :error_type,
                    last_error_message = :error_message,
                    updated_at = :updated_at
            """,
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues=to_decimal({
                ":failed": "FAILED",
                ":processed_count": processed_count,
                ":success_count": success_count,
                ":failed_count": failed_count,
                ":api_calls_count": api_calls_count,
                ":error_type": type(error).__name__,
                ":error_message": str(error),
                ":updated_at": utc_now_iso()
            })
        )
    except Exception:
        pass


def read_partition_csv(input_path: str) -> List[Dict[str, Any]]:
    location = parse_s3_uri(input_path)

    response = s3.get_object(
        Bucket=location["bucket"],
        Key=location["key"]
    )

    text = response["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(text))

    if not reader.fieldnames:
        raise ValueError(f"Partition CSV has no header: {input_path}")

    if "model_id" not in reader.fieldnames:
        raise ValueError(f"Partition CSV must contain model_id: {input_path}")

    return list(reader)


def put_jsonl_to_s3(rows: List[Dict[str, Any]], bucket: str, key: str) -> None:
    body = "\n".join(
        json.dumps(row, ensure_ascii=False, default=str)
        for row in rows
    )

    if body:
        body += "\n"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson"
    )


def put_json_to_s3(payload: Dict[str, Any], bucket: str, key: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
        ContentType="application/json"
    )


def put_success_marker(bucket: str, key: str, payload: Dict[str, Any]) -> None:
    put_json_to_s3(payload, bucket, key)


def fetch_hf_model(model_id: str, hf_token: str) -> Dict[str, Any]:
    encoded_model_id = quote(model_id, safe="/")
    url = f"{HF_MODEL_API_BASE_URL}/{encoded_model_id}?full=true&cardData=true"

    request = Request(
        url=url,
        headers={
            "Authorization": f"Bearer {hf_token}",
            "Accept": "application/json"
        },
        method="GET"
    )

    try:
        with urlopen(request, timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))

    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HF API HTTP {exc.code} for model_id={model_id}: {body[:500]}")

    except URLError as exc:
        raise RuntimeError(f"HF API URL error for model_id={model_id}: {exc}")


def safe_json(value: Any) -> str:
    if value is None:
        return ""

    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError:
        return json.dumps(str(value), ensure_ascii=False)


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


def extract_card_data(hf_payload: Dict[str, Any]) -> Dict[str, Any]:
    card_data = (
        hf_payload.get("cardData")
        or hf_payload.get("card_data")
        or {}
    )

    if not isinstance(card_data, dict):
        return {}

    return card_data


def extract_datasets(card_data: Dict[str, Any], hf_payload: Dict[str, Any]) -> List[Any]:
    datasets = (
        card_data.get("datasets")
        or card_data.get("dataset")
        or hf_payload.get("datasets")
        or []
    )

    if isinstance(datasets, list):
        return datasets

    if isinstance(datasets, str):
        return [datasets]

    return []


def extract_performance_metrics(card_data: Dict[str, Any]) -> Any:
    for key in ("model-index", "model_index", "eval_results", "metrics", "performance"):
        value = card_data.get(key)
        if value:
            return value

    return None


def extract_performance_score(metrics: Any) -> Optional[float]:
    if metrics is None:
        return None

    numbers: List[float] = []

    def collect(value: Any) -> None:
        if isinstance(value, (int, float)):
            numbers.append(float(value))
        elif isinstance(value, dict):
            for nested in value.values():
                collect(nested)
        elif isinstance(value, list):
            for nested in value:
                collect(nested)

    collect(metrics)

    if not numbers:
        return None

    return max(numbers)


def extract_model_size(hf_payload: Dict[str, Any]) -> Optional[int]:
    safetensors = hf_payload.get("safetensors")

    if isinstance(safetensors, dict):
        total = safetensors.get("total")
        if isinstance(total, int):
            return total

    siblings = hf_payload.get("siblings") or []

    total_size = 0
    found = False

    if isinstance(siblings, list):
        for item in siblings:
            if isinstance(item, dict) and isinstance(item.get("size"), int):
                total_size += item["size"]
                found = True

    return total_size if found else None


def infer_domain(row: Dict[str, Any], hf_payload: Dict[str, Any]) -> Optional[str]:
    pipeline_tag = row.get("pipeline_tag") or hf_payload.get("pipeline_tag")

    if pipeline_tag:
        return str(pipeline_tag)

    tags = hf_payload.get("tags") or row.get("tags")

    if isinstance(tags, list) and tags:
        return str(tags[0])

    if isinstance(tags, str) and tags:
        return tags[:100]

    return None


def infer_auto(row: Dict[str, Any], hf_payload: Dict[str, Any]) -> bool:
    tags = hf_payload.get("tags") or row.get("tags") or []

    if isinstance(tags, str):
        tags_text = tags.lower()
    else:
        tags_text = " ".join(str(tag).lower() for tag in tags)

    return "autotrain" in tags_text or "auto" in tags_text


def build_enriched_row(
    row: Dict[str, Any],
    hf_payload: Dict[str, Any],
    run_id: str,
    partition_id: str
) -> Dict[str, Any]:
    card_data = extract_card_data(hf_payload)

    datasets = extract_datasets(card_data, hf_payload)
    datasets_size = len(datasets)

    performance_metrics = extract_performance_metrics(card_data)
    performance_score = extract_performance_score(performance_metrics)

    size = extract_model_size(hf_payload)

    downloads = safe_int(hf_payload.get("downloads") or row.get("downloads"))
    likes = safe_int(hf_payload.get("likes") or row.get("likes"))

    co2_eq_emissions = safe_float(row.get("co2_eq_emissions"))

    size_efficency = None
    if size and size > 0:
        size_efficency = downloads / size

    datasets_size_efficency = None
    if datasets_size > 0:
        datasets_size_efficency = downloads / datasets_size

    enriched = {
        "modelId": hf_payload.get("id") or row.get("model_id"),
        "datasets": safe_json(datasets),
        "datasets_size": datasets_size,

        "co2_eq_emissions": co2_eq_emissions,
        "co2_reported": co2_eq_emissions is not None,
        "source": row.get("co2_source"),

        "training_type": row.get("training_type"),
        "geographical_location": row.get("geographical_location"),
        "environment": card_data.get("environment"),

        "performance_metrics": safe_json(performance_metrics),
        "performance_score": performance_score,

        "downloads": downloads,
        "likes": likes,
        "library_name": hf_payload.get("library_name") or row.get("library_name"),

        "domain": infer_domain(row, hf_payload),
        "size": size,
        "created_at": hf_payload.get("createdAt") or hf_payload.get("created_at") or row.get("created_at"),

        "size_efficency": size_efficency,
        "datasets_size_efficency": datasets_size_efficency,
        "auto": infer_auto(row, hf_payload),

        # Campos de trazabilidad del enrichment
        "run_id": run_id,
        "partition_id": partition_id,
        "enriched_at": utc_now_iso(),

        # Campos originales del discovery
        "model_id": row.get("model_id"),
        "co2_source": row.get("co2_source"),
        "hardware_used": row.get("hardware_used"),
        "pipeline_tag": row.get("pipeline_tag"),
        "tags": row.get("tags"),
        "snapshot_id": row.get("snapshot_id"),
        "discovered_at": row.get("discovered_at"),
    }

    return enriched


def build_error_row(
    row: Dict[str, Any],
    error: Exception,
    run_id: str,
    partition_id: str
) -> Dict[str, Any]:
    return {
        "run_id": run_id,
        "partition_id": partition_id,
        "model_id": row.get("model_id"),
        "error_type": type(error).__name__,
        "error_message": str(error),
        "failed_at": utc_now_iso(),
    }


def sleep_if_window_budget_reached(
    api_calls_in_window: int,
    partition_budget: int,
    window_started_at: float,
    window_seconds: int
) -> tuple[int, float]:
    if api_calls_in_window < partition_budget:
        return api_calls_in_window, window_started_at

    elapsed = time.time() - window_started_at
    remaining = window_seconds - elapsed

    if remaining > 0:
        print(f"Rate window budget reached. Sleeping {remaining:.2f} seconds.")
        time.sleep(remaining)

    return 0, time.time()


sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


run_id = args["run_id"]
partition_id = normalize_partition_id(args["partition_id"])
table_name = args["control_table_name"]

processed_count = 0
success_count = 0
failed_count = 0
api_calls_count = 0
batches_written = 0
last_batch_path = None

try:
    hf_token = get_hf_token(args["hf_token_secret_name"])

    partition_config = get_partition_config(
        table_name=table_name,
        run_id=run_id,
        partition_id=partition_id
    )

    mark_partition_running(
        table_name=table_name,
        run_id=run_id,
        partition_id=partition_id,
        job_name=args["JOB_NAME"]
    )

    input_path = partition_config["input_path"]
    records_count = int(partition_config.get("records_count", 0))

    bucket = parse_s3_uri(input_path)["bucket"]

    output_prefix = (
        f"enriched/hf-carbon/run_id={run_id}/partition_id={partition_id}"
    )

    rows = read_partition_csv(input_path)

    if records_count and records_count != len(rows):
        print(
            f"Warning: records_count mismatch. "
            f"DynamoDB={records_count}, CSV={len(rows)}"
        )

    partition_budget = math_budget = max(1, int(1000 / 4))
    window_seconds = DEFAULT_WINDOW_SECONDS

    if "rate_budget" in partition_config:
        rate_budget = partition_config["rate_budget"]
        partition_budget = int(rate_budget.get("partition_call_budget", partition_budget))
        window_seconds = int(rate_budget.get("window_seconds", DEFAULT_WINDOW_SECONDS))

    batch: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    window_started_at = time.time()
    api_calls_in_window = 0

    for row in rows:
        model_id = str(row.get("model_id") or "").strip()

        if not model_id:
            failed_count += 1
            processed_count += 1
            errors.append(
                build_error_row(
                    row=row,
                    error=ValueError("Missing model_id"),
                    run_id=run_id,
                    partition_id=partition_id
                )
            )
            continue

        api_calls_in_window, window_started_at = sleep_if_window_budget_reached(
            api_calls_in_window=api_calls_in_window,
            partition_budget=partition_budget,
            window_started_at=window_started_at,
            window_seconds=window_seconds
        )

        try:
            hf_payload = fetch_hf_model(model_id, hf_token)
            api_calls_count += 1
            api_calls_in_window += 1

            enriched_row = build_enriched_row(
                row=row,
                hf_payload=hf_payload,
                run_id=run_id,
                partition_id=partition_id
            )

            batch.append(enriched_row)
            success_count += 1

        except Exception as exc:
            api_calls_count += 1
            api_calls_in_window += 1

            errors.append(
                build_error_row(
                    row=row,
                    error=exc,
                    run_id=run_id,
                    partition_id=partition_id
                )
            )
            failed_count += 1

        processed_count += 1

        if len(batch) >= DEFAULT_BATCH_SIZE:
            batches_written += 1
            batch_key = (
                f"{output_prefix}/batches/batch_{batches_written:06d}.jsonl"
            )

            put_jsonl_to_s3(
                rows=batch,
                bucket=bucket,
                key=batch_key
            )

            last_batch_path = f"s3://{bucket}/{batch_key}"
            batch = []

            update_partition_progress(
                table_name=table_name,
                run_id=run_id,
                partition_id=partition_id,
                processed_count=processed_count,
                success_count=success_count,
                failed_count=failed_count,
                api_calls_count=api_calls_count,
                batches_written=batches_written,
                last_batch_path=last_batch_path
            )

    if batch:
        batches_written += 1
        batch_key = f"{output_prefix}/batches/batch_{batches_written:06d}.jsonl"

        put_jsonl_to_s3(
            rows=batch,
            bucket=bucket,
            key=batch_key
        )

        last_batch_path = f"s3://{bucket}/{batch_key}"

    if errors:
        errors_key = f"{output_prefix}/errors/errors.jsonl"

        put_jsonl_to_s3(
            rows=errors,
            bucket=bucket,
            key=errors_key
        )

    metrics = {
        "run_id": run_id,
        "partition_id": partition_id,
        "input_count": len(rows),
        "processed_count": processed_count,
        "success_count": success_count,
        "failed_count": failed_count,
        "api_calls_count": api_calls_count,
        "batches_written": batches_written,
        "completed_at": utc_now_iso(),
    }

    metrics_key = f"{output_prefix}/metrics/metrics.json"
    put_json_to_s3(metrics, bucket, metrics_key)


    if api_calls_in_window >= partition_budget:
        sleep_if_window_budget_reached(
            api_calls_in_window=api_calls_in_window,
            partition_budget=partition_budget,
            window_started_at=window_started_at,
            window_seconds=window_seconds
        )

    success_key = f"{output_prefix}/_SUCCESS"

    success_payload = {
        "run_id": run_id,
        "partition_id": partition_id,
        "status": "COMPLETED",
        "input_count": len(rows),
        "success_count": success_count,
        "failed_count": failed_count,
        "api_calls_count": api_calls_count,
        "batches_written": batches_written,
        "success_marker_created_at": utc_now_iso(),
    }

    put_success_marker(bucket, success_key, success_payload)


    def mark_partition_completed(
            table_name: str,
            run_id: str,
            partition_id: str,
            output_prefix: str,
            success_marker_path: str,
            input_count: int,
            success_count: int,
            failed_count: int,
            api_calls_count: int,
            batches_written: int
    ) -> None:
        table = dynamodb.Table(table_name)

        table.update_item(
            Key={
                "PK": f"RUN#{run_id}",
                "SK": f"PARTITION#{partition_id}"
            },
            UpdateExpression="""
                SET #status = :completed,
                    output_prefix = :output_prefix,
                    success_marker_path = :success_marker_path,
                    input_count = :input_count,
                    processed_count = :processed_count,
                    success_count = :success_count,
                    failed_count = :failed_count,
                    api_calls_count = :api_calls_count,
                    batches_written = :batches_written,
                    updated_at = :updated_at,
                    completed_at = :completed_at
            """,
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues=to_decimal({
                ":completed": "COMPLETED",
                ":output_prefix": output_prefix,
                ":success_marker_path": success_marker_path,
                ":input_count": input_count,
                ":processed_count": success_count + failed_count,
                ":success_count": success_count,
                ":failed_count": failed_count,
                ":api_calls_count": api_calls_count,
                ":batches_written": batches_written,
                ":updated_at": utc_now_iso(),
                ":completed_at": utc_now_iso()
            })
        )

finally:
    job.commit()