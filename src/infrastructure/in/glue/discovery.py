import csv
import io
import json
import os
import re
import tempfile
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import boto3
import requests
from botocore.exceptions import ClientError


# =============================================================================
# Environment configuration
# =============================================================================

TARGET_BUCKET = os.environ["CUSTOMER_TARGET_BUCKET_NAME"]
HF_TOKEN_SECRET_NAME = os.environ["CUSTOMER_HF_TOKEN_SECRET_NAME"]

BASE_PREFIX = "discovery/hf-carbon"
CHECKPOINT_KEY = f"{BASE_PREFIX}/checkpoints/latest.json"

HF_MODELS_URL = "https://huggingface.co/api/models"

PAGE_LIMIT = 1000
REQUEST_TIMEOUT_SECONDS = 60

FLUSH_EVERY_PAGES = 5
FLUSH_EVERY_MATCHES = 100

MAX_429_RETRIES_PER_RUN = 5
DEFAULT_429_SLEEP_SECONDS = 180

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


s3 = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")


# =============================================================================
# Time helpers
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def utc_now_compact() -> str:
    return utc_now().strftime("%Y%m%dT%H%M%SZ")


# =============================================================================
# Secrets
# =============================================================================

def get_hf_token_from_secrets_manager(secret_name: str) -> str:
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        raise RuntimeError(
            f"Could not read Hugging Face token from Secrets Manager secret: {secret_name}"
        ) from exc

    secret_string = response.get("SecretString")

    if not secret_string:
        raise ValueError(
            f"Secret {secret_name} does not contain SecretString. Binary secrets are not supported."
        )

    if not secret_string.strip().startswith("{"):
        return secret_string.strip()

    secret_json = json.loads(secret_string)

    for key in ("HF_TOKEN", "hf_token", "token"):
        token = secret_json.get(key)
        if token:
            return str(token).strip()

    raise ValueError(
        f"Secret {secret_name} is JSON but does not contain one of: HF_TOKEN, hf_token, token"
    )


# =============================================================================
# S3 helpers
# =============================================================================

def s3_object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def read_json_from_s3(bucket: str, key: str) -> Optional[Dict[str, Any]]:
    if not s3_object_exists(bucket, key):
        return None

    response = s3.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read().decode("utf-8")
    return json.loads(body)


def upload_json_to_s3(payload: Dict[str, Any], bucket: str, key: str) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def upload_text_to_s3(text: str, bucket: str, key: str, content_type: str = "text/plain") -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType=content_type,
    )


def list_s3_keys(bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            keys.append(item["Key"])

    return keys


def read_s3_text(bucket: str, key: str) -> str:
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")


# =============================================================================
# Hugging Face parsing
# =============================================================================

def to_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    return str(value)


def safe_json(value: Any) -> str:
    if value is None:
        return ""

    try:
        return json.dumps(value, ensure_ascii=False)
    except TypeError:
        return json.dumps(str(value), ensure_ascii=False)


def normalize_co2_emissions(value: Any) -> Optional[float]:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        cleaned = value.strip().lower()
        cleaned = cleaned.replace("kg", "")
        cleaned = cleaned.replace("grams", "")
        cleaned = cleaned.replace("gram", "")
        cleaned = cleaned.replace("g", "")
        cleaned = cleaned.replace("co2eq", "")
        cleaned = cleaned.replace("co2e", "")
        cleaned = cleaned.replace("co₂eq", "")
        cleaned = cleaned.replace("co₂e", "")
        cleaned = cleaned.replace(",", "")
        cleaned = cleaned.strip()

        try:
            return float(cleaned)
        except ValueError:
            return None

    return None


def extract_co2_fields(card_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    co2 = card_data.get("co2_eq_emissions")

    if not co2 or not isinstance(co2, dict):
        return None

    emissions = normalize_co2_emissions(co2.get("emissions"))

    if emissions is None:
        return None

    return {
        "co2_eq_emissions": emissions,
        "co2_source": co2.get("source"),
        "training_type": co2.get("training_type"),
        "geographical_location": co2.get("geographical_location"),
        "hardware_used": co2.get("hardware_used"),
    }


def model_to_row(model: Dict[str, Any], snapshot_id: str, discovered_at: str) -> Optional[Dict[str, Any]]:
    card_data = model.get("cardData") or model.get("card_data") or {}

    if not isinstance(card_data, dict):
        return None

    co2_fields = extract_co2_fields(card_data)

    if co2_fields is None:
        return None

    return {
        "model_id": model.get("id") or model.get("modelId"),
        "co2_eq_emissions": co2_fields["co2_eq_emissions"],
        "co2_source": co2_fields["co2_source"],
        "training_type": co2_fields["training_type"],
        "geographical_location": co2_fields["geographical_location"],
        "hardware_used": co2_fields["hardware_used"],
        "created_at": to_iso(model.get("createdAt") or model.get("created_at")),
        "downloads": model.get("downloads"),
        "likes": model.get("likes"),
        "library_name": model.get("library_name") or model.get("libraryName"),
        "pipeline_tag": model.get("pipeline_tag") or model.get("pipelineTag"),
        "tags": safe_json(model.get("tags")),
        "snapshot_id": snapshot_id,
        "discovered_at": discovered_at,
    }


# =============================================================================
# CSV helpers
# =============================================================================

def rows_to_csv_text(rows: List[Dict[str, Any]]) -> str:
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def upload_rows_part(
    snapshot_id: str,
    part_number: int,
    rows: List[Dict[str, Any]],
) -> Optional[str]:
    if not rows:
        return None

    key = (
        f"{BASE_PREFIX}/snapshot_id={snapshot_id}/filtered_parts/"
        f"models_with_emissions_part_{part_number:06d}.csv"
    )

    csv_text = rows_to_csv_text(rows)
    upload_text_to_s3(csv_text, TARGET_BUCKET, key, content_type="text/csv")

    return f"s3://{TARGET_BUCKET}/{key}"


def consolidate_parts(snapshot_id: str) -> Tuple[str, int]:
    parts_prefix = f"{BASE_PREFIX}/snapshot_id={snapshot_id}/filtered_parts/"
    part_keys = sorted(
        key for key in list_s3_keys(TARGET_BUCKET, parts_prefix)
        if key.endswith(".csv")
    )

    deduped: Dict[str, Dict[str, Any]] = {}

    for key in part_keys:
        text = read_s3_text(TARGET_BUCKET, key)
        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            model_id = row.get("model_id")
            if not model_id:
                continue
            deduped[model_id] = row

    final_rows = list(deduped.values())

    final_csv = rows_to_csv_text(final_rows)

    snapshot_key = f"{BASE_PREFIX}/snapshot_id={snapshot_id}/filtered/models_with_emissions.csv"
    latest_key = f"{BASE_PREFIX}/latest/models_with_emissions.csv"

    upload_text_to_s3(final_csv, TARGET_BUCKET, snapshot_key, content_type="text/csv")
    upload_text_to_s3(final_csv, TARGET_BUCKET, latest_key, content_type="text/csv")

    return f"s3://{TARGET_BUCKET}/{snapshot_key}", len(final_rows)


# =============================================================================
# Cursor pagination helpers
# =============================================================================

def extract_next_cursor_from_link_header(link_header: Optional[str]) -> Optional[str]:
    if not link_header:
        return None

    links = [part.strip() for part in link_header.split(",")]

    for link in links:
        if 'rel="next"' not in link:
            continue

        match = re.search(r"<([^>]+)>", link)
        if not match:
            continue

        next_url = match.group(1)
        parsed = urlparse(next_url)
        query = parse_qs(parsed.query)
        cursor_values = query.get("cursor")

        if cursor_values:
            return cursor_values[0]

    return None


def fetch_models_page(
    session: requests.Session,
    hf_token: str,
    cursor: Optional[str],
) -> Tuple[List[Dict[str, Any]], Optional[str], Dict[str, str]]:
    headers = {
        "Authorization": f"Bearer {hf_token}",
        "Accept": "application/json",
    }

    params = {
        "full": "true",
        "cardData": "true",
        "sort": "trendingScore",
        "limit": str(PAGE_LIMIT),
    }

    if cursor:
        params["cursor"] = cursor

    response = session.get(
        HF_MODELS_URL,
        headers=headers,
        params=params,
        timeout=REQUEST_TIMEOUT_SECONDS,
    )

    response_headers = dict(response.headers)

    if response.status_code == 429:
        raise RateLimitError(
            status_code=429,
            retry_after=response.headers.get("Retry-After"),
            response_text=response.text,
            headers=response_headers,
        )

    if response.status_code >= 400:
        raise RuntimeError(
            f"Hugging Face API error {response.status_code}: {response.text[:1000]}"
        )

    payload = response.json()

    if not isinstance(payload, list):
        raise RuntimeError(
            f"Unexpected Hugging Face response. Expected list, got: {type(payload).__name__}"
        )

    next_cursor = extract_next_cursor_from_link_header(response.headers.get("Link"))

    return payload, next_cursor, response_headers


class RateLimitError(Exception):
    def __init__(
        self,
        status_code: int,
        retry_after: Optional[str],
        response_text: str,
        headers: Dict[str, str],
    ):
        self.status_code = status_code
        self.retry_after = retry_after
        self.response_text = response_text
        self.headers = headers
        super().__init__(
            f"Rate limited with status {status_code}. Retry-After={retry_after}"
        )


def retry_after_to_seconds(value: Optional[str]) -> int:
    if not value:
        return DEFAULT_429_SLEEP_SECONDS

    try:
        parsed = int(float(value))
        return max(parsed, 1)
    except ValueError:
        return DEFAULT_429_SLEEP_SECONDS


# =============================================================================
# Checkpoint logic
# =============================================================================

def load_or_create_checkpoint() -> Dict[str, Any]:
    existing = read_json_from_s3(TARGET_BUCKET, CHECKPOINT_KEY)

    if existing and existing.get("status") in {
        "RUNNING",
        "FAILED",
        "RATE_LIMITED",
        "INTERRUPTED",
    }:
        print(
            json.dumps(
                {
                    "event": "checkpoint_found",
                    "snapshot_id": existing.get("snapshot_id"),
                    "next_cursor_present": bool(existing.get("next_cursor")),
                    "last_successful_page": existing.get("last_successful_page"),
                    "total_models_seen": existing.get("total_models_seen"),
                    "models_with_emissions": existing.get("models_with_emissions"),
                },
                ensure_ascii=False,
            )
        )
        return existing

    snapshot_id = utc_now_compact()

    checkpoint = {
        "snapshot_id": snapshot_id,
        "status": "RUNNING",
        "next_cursor": None,
        "last_successful_page": 0,
        "total_models_seen": 0,
        "models_with_emissions": 0,
        "part_number": 0,
        "started_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
    }

    save_checkpoint(checkpoint)

    print(
        json.dumps(
            {
                "event": "checkpoint_created",
                "snapshot_id": snapshot_id,
            },
            ensure_ascii=False,
        )
    )

    return checkpoint


def save_checkpoint(checkpoint: Dict[str, Any]) -> None:
    checkpoint["updated_at"] = utc_now_iso()

    upload_json_to_s3(
        checkpoint,
        TARGET_BUCKET,
        CHECKPOINT_KEY,
    )

    snapshot_id = checkpoint["snapshot_id"]

    upload_json_to_s3(
        checkpoint,
        TARGET_BUCKET,
        f"{BASE_PREFIX}/snapshot_id={snapshot_id}/_metadata/checkpoint.json",
    )


def save_progress_event(
    snapshot_id: str,
    event_name: str,
    payload: Dict[str, Any],
) -> None:
    key = f"{BASE_PREFIX}/snapshot_id={snapshot_id}/_metadata/{event_name}.json"
    upload_json_to_s3(payload, TARGET_BUCKET, key)


# =============================================================================
# Main discovery
# =============================================================================

def run_discovery() -> Dict[str, Any]:
    hf_token = get_hf_token_from_secrets_manager(HF_TOKEN_SECRET_NAME)

    checkpoint = load_or_create_checkpoint()

    snapshot_id = checkpoint["snapshot_id"]
    discovered_at = checkpoint.get("started_at") or utc_now_iso()

    next_cursor = checkpoint.get("next_cursor")
    page_number = int(checkpoint.get("last_successful_page") or 0)
    total_models_seen = int(checkpoint.get("total_models_seen") or 0)
    models_with_emissions = int(checkpoint.get("models_with_emissions") or 0)
    part_number = int(checkpoint.get("part_number") or 0)

    pending_rows: List[Dict[str, Any]] = []
    rate_limit_retries = 0

    session = requests.Session()

    print(
        json.dumps(
            {
                "event": "discovery_resumed_or_started",
                "snapshot_id": snapshot_id,
                "starting_page": page_number + 1,
                "total_models_seen": total_models_seen,
                "models_with_emissions": models_with_emissions,
                "has_cursor": bool(next_cursor),
            },
            ensure_ascii=False,
        )
    )

    try:
        while True:
            try:
                models, new_next_cursor, response_headers = fetch_models_page(
                    session=session,
                    hf_token=hf_token,
                    cursor=next_cursor,
                )
                rate_limit_retries = 0

            except RateLimitError as exc:
                checkpoint["status"] = "RATE_LIMITED"
                checkpoint["next_cursor"] = next_cursor
                checkpoint["last_successful_page"] = page_number
                checkpoint["total_models_seen"] = total_models_seen
                checkpoint["models_with_emissions"] = models_with_emissions
                checkpoint["part_number"] = part_number
                checkpoint["last_error"] = str(exc)
                checkpoint["last_retry_after"] = exc.retry_after
                save_checkpoint(checkpoint)

                if pending_rows:
                    part_number += 1
                    part_uri = upload_rows_part(snapshot_id, part_number, pending_rows)
                    pending_rows = []

                    checkpoint["part_number"] = part_number
                    checkpoint["last_part_uri"] = part_uri
                    save_checkpoint(checkpoint)

                save_progress_event(
                    snapshot_id=snapshot_id,
                    event_name="rate_limited",
                    payload={
                        "snapshot_id": snapshot_id,
                        "status": "RATE_LIMITED",
                        "retry_after": exc.retry_after,
                        "sleep_seconds": retry_after_to_seconds(exc.retry_after),
                        "total_models_seen": total_models_seen,
                        "models_with_emissions": models_with_emissions,
                        "page_number": page_number,
                        "cursor_saved": bool(next_cursor),
                        "event_at": utc_now_iso(),
                    },
                )

                rate_limit_retries += 1

                if rate_limit_retries > MAX_429_RETRIES_PER_RUN:
                    checkpoint["status"] = "INTERRUPTED"
                    checkpoint["last_error"] = (
                        f"Exceeded MAX_429_RETRIES_PER_RUN={MAX_429_RETRIES_PER_RUN}"
                    )
                    save_checkpoint(checkpoint)
                    raise

                sleep_seconds = retry_after_to_seconds(exc.retry_after)

                print(
                    json.dumps(
                        {
                            "event": "rate_limited_sleeping",
                            "sleep_seconds": sleep_seconds,
                            "retry": rate_limit_retries,
                            "max_retries": MAX_429_RETRIES_PER_RUN,
                            "snapshot_id": snapshot_id,
                        },
                        ensure_ascii=False,
                    )
                )

                time.sleep(sleep_seconds)
                continue

            if not models:
                print(
                    json.dumps(
                        {
                            "event": "empty_page_received",
                            "snapshot_id": snapshot_id,
                            "page_number": page_number + 1,
                        },
                        ensure_ascii=False,
                    )
                )
                break

            page_matches = 0

            for model in models:
                total_models_seen += 1

                row = model_to_row(
                    model=model,
                    snapshot_id=snapshot_id,
                    discovered_at=discovered_at,
                )

                if row is None:
                    continue

                pending_rows.append(row)
                models_with_emissions += 1
                page_matches += 1

            page_number += 1

            next_cursor = new_next_cursor

            checkpoint["status"] = "RUNNING"
            checkpoint["next_cursor"] = next_cursor
            checkpoint["last_successful_page"] = page_number
            checkpoint["total_models_seen"] = total_models_seen
            checkpoint["models_with_emissions"] = models_with_emissions
            checkpoint["part_number"] = part_number
            checkpoint["last_rate_limit"] = response_headers.get("RateLimit")
            checkpoint["last_rate_limit_policy"] = response_headers.get("RateLimit-Policy")
            save_checkpoint(checkpoint)

            should_flush_by_pages = page_number % FLUSH_EVERY_PAGES == 0
            should_flush_by_matches = len(pending_rows) >= FLUSH_EVERY_MATCHES

            if pending_rows and (should_flush_by_pages or should_flush_by_matches):
                part_number += 1
                part_uri = upload_rows_part(snapshot_id, part_number, pending_rows)
                pending_rows = []

                checkpoint["part_number"] = part_number
                checkpoint["last_part_uri"] = part_uri
                save_checkpoint(checkpoint)

                print(
                    json.dumps(
                        {
                            "event": "part_flushed",
                            "snapshot_id": snapshot_id,
                            "part_number": part_number,
                            "part_uri": part_uri,
                            "page_number": page_number,
                            "total_models_seen": total_models_seen,
                            "models_with_emissions": models_with_emissions,
                        },
                        ensure_ascii=False,
                    )
                )

            print(
                json.dumps(
                    {
                        "event": "page_processed",
                        "snapshot_id": snapshot_id,
                        "page_number": page_number,
                        "page_size": len(models),
                        "page_matches": page_matches,
                        "total_models_seen": total_models_seen,
                        "models_with_emissions": models_with_emissions,
                        "has_next_cursor": bool(next_cursor),
                    },
                    ensure_ascii=False,
                )
            )

            if not next_cursor:
                break

        if pending_rows:
            part_number += 1
            part_uri = upload_rows_part(snapshot_id, part_number, pending_rows)
            pending_rows = []

            checkpoint["part_number"] = part_number
            checkpoint["last_part_uri"] = part_uri
            save_checkpoint(checkpoint)

        snapshot_path, final_rows_count = consolidate_parts(snapshot_id)

        result = {
            "snapshot_id": snapshot_id,
            "status": "COMPLETED",
            "snapshot_path": snapshot_path,
            "latest_path": f"s3://{TARGET_BUCKET}/{BASE_PREFIX}/latest/models_with_emissions.csv",
            "total_models_seen": total_models_seen,
            "models_with_emissions": models_with_emissions,
            "final_rows_count": final_rows_count,
            "part_number": part_number,
            "completed_at": utc_now_iso(),
        }

        checkpoint["status"] = "COMPLETED"
        checkpoint["next_cursor"] = None
        checkpoint["last_successful_page"] = page_number
        checkpoint["total_models_seen"] = total_models_seen
        checkpoint["models_with_emissions"] = models_with_emissions
        checkpoint["part_number"] = part_number
        checkpoint["result"] = result
        save_checkpoint(checkpoint)

        save_progress_event(
            snapshot_id=snapshot_id,
            event_name="result",
            payload=result,
        )

        print(json.dumps(result, ensure_ascii=False))
        return result

    except Exception as exc:
        if pending_rows:
            part_number += 1
            part_uri = upload_rows_part(snapshot_id, part_number, pending_rows)
            checkpoint["part_number"] = part_number
            checkpoint["last_part_uri"] = part_uri

        checkpoint["status"] = "FAILED"
        checkpoint["next_cursor"] = next_cursor
        checkpoint["last_successful_page"] = page_number
        checkpoint["total_models_seen"] = total_models_seen
        checkpoint["models_with_emissions"] = models_with_emissions
        checkpoint["last_error"] = str(exc)
        checkpoint["failed_at"] = utc_now_iso()
        save_checkpoint(checkpoint)

        save_progress_event(
            snapshot_id=snapshot_id,
            event_name="error",
            payload={
                "snapshot_id": snapshot_id,
                "status": "FAILED",
                "error_message": str(exc),
                "total_models_seen": total_models_seen,
                "models_with_emissions": models_with_emissions,
                "page_number": page_number,
                "next_cursor_saved": bool(next_cursor),
                "failed_at": utc_now_iso(),
            },
        )

        raise


if __name__ == "__main__":
    run_discovery()