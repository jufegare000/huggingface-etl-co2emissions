import csv
import io
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import boto3
import requests
from botocore.exceptions import ClientError

from data_preparation.application.services.config.environment.env_variables_service_implemented import \
    EnvironmentVariablesService
from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from shared.domain.exceptions.rate_limit_error import RateLimitError

env_service = EnvironmentVariablesService()
logger = logging.getLogger(__name__)


class DiscoveryJobService():

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

    def utc_now(self) -> datetime:
        return datetime.now(timezone.utc)


    def utc_now_iso(self) -> str:
        return self.utc_now().isoformat()


    def utc_now_compact(self) -> str:
        return self.utc_now().strftime("%Y%m%dT%H%M%SZ")


    # =============================================================================
    # Secrets
    # =============================================================================

    def get_hf_token_from_secrets_manager(self, secret_name: str) -> str:
        try:
            response = self.secrets_client.get_secret_value(SecretId=secret_name)
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

    def s3_object_exists(self, bucket: str, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise


    def read_json_from_s3(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        if not self.s3_object_exists(bucket, key):
            return None

        response = self.s3.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read().decode("utf-8")
        return json.loads(body)


    def upload_json_to_s3(self, payload: Dict[str, Any], bucket: str, key: str) -> None:
        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
            ContentType="application/json",
        )


    def upload_text_to_s3(self, text: str, bucket: str, key: str, content_type: str = "text/plain") -> None:
        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=text.encode("utf-8"),
            ContentType=content_type,
        )


    def list_s3_keys(self, bucket: str, prefix: str) -> List[str]:
        keys: List[str] = []
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                keys.append(item["Key"])

        return keys


    def read_s3_text(self, bucket: str, key: str) -> str:
        response = self.s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")


    # =============================================================================
    # Hugging Face parsing
    # =============================================================================

    def to_iso(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        return str(value)


    def safe_json(self, value: Any) -> str:
        if value is None:
            return ""

        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return json.dumps(str(value), ensure_ascii=False)


    def normalize_co2_emissions(self, value: Any) -> Optional[float]:
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
            cleaned = cleaned.replace("co?eq", "")
            cleaned = cleaned.replace("co?e", "")
            cleaned = cleaned.replace(",", "")
            cleaned = cleaned.strip()

            try:
                return float(cleaned)
            except ValueError:
                return None

        return None


    def extract_co2_fields(self, card_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        co2 = card_data.get("co2_eq_emissions")

        if not co2 or not isinstance(co2, dict):
            return None

        emissions = self.normalize_co2_emissions(co2.get("emissions"))

        if emissions is None:
            return None

        return {
            "co2_eq_emissions": emissions,
            "co2_source": co2.get("source"),
            "training_type": co2.get("training_type"),
            "geographical_location": co2.get("geographical_location"),
            "hardware_used": co2.get("hardware_used"),
        }


    def model_to_row(self, model: Dict[str, Any], snapshot_id: str, discovered_at: str) -> Optional[Dict[str, Any]]:
        card_data = model.get("cardData") or model.get("card_data") or {}

        if not isinstance(card_data, dict):
            return None

        co2_fields = self.extract_co2_fields(card_data)

        if co2_fields is None:
            return None

        return {
            "model_id": model.get("id") or model.get("modelId"),
            "co2_eq_emissions": co2_fields["co2_eq_emissions"],
            "co2_source": co2_fields["co2_source"],
            "training_type": co2_fields["training_type"],
            "geographical_location": co2_fields["geographical_location"],
            "hardware_used": co2_fields["hardware_used"],
            "created_at": self.to_iso(model.get("createdAt") or model.get("created_at")),
            "downloads": model.get("downloads"),
            "likes": model.get("likes"),
            "library_name": model.get("library_name") or model.get("libraryName"),
            "pipeline_tag": model.get("pipeline_tag") or model.get("pipelineTag"),
            "tags": self.safe_json(model.get("tags")),
            "snapshot_id": snapshot_id,
            "discovered_at": discovered_at,
        }


    # =============================================================================
    # CSV helpers
    # =============================================================================

    def rows_to_csv_text(self, rows: List[Dict[str, Any]]) -> str:
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=self.CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
        return buffer.getvalue()


    def get_target_bucket(self):
        return str(env_service.load_env_variable("CUSTOMER_TARGET_BUCKET_NAME"))


    def upload_rows_part(
            self,
            snapshot_id: str,
            part_number: int,
            rows: List[Dict[str, Any]],
    ) -> Optional[str]:
        if not rows:
            return None

        key = (
            f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={snapshot_id}/filtered_parts/"
            f"models_with_emissions_part_{part_number:06d}.csv"
        )

        csv_text = self.rows_to_csv_text(rows)
        target_bucket = self.get_target_bucket()
        self.upload_text_to_s3(csv_text, target_bucket, key, content_type="text/csv")

        return f"s3://{target_bucket}/{key}"


    def consolidate_parts(self, snapshot_id: str) -> Tuple[str, int]:
        parts_prefix = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={snapshot_id}/filtered_parts/"
        part_keys = sorted(
            key for key in self.list_s3_keys(self.get_target_bucket(), parts_prefix)
            if key.endswith(".csv")
        )

        deduped: Dict[str, Dict[str, Any]] = {}

        for key in part_keys:
            text = self.read_s3_text(self.get_target_bucket(), key)
            reader = csv.DictReader(io.StringIO(text))

            for row in reader:
                model_id = row.get("model_id")
                if not model_id:
                    continue
                deduped[model_id] = row

        final_rows = list(deduped.values())
        final_csv = self.rows_to_csv_text(final_rows)

        snapshot_key = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={snapshot_id}/filtered/models_with_emissions.csv"
        latest_key = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/latest/models_with_emissions.csv"

        self.upload_text_to_s3(final_csv, self.get_target_bucket(), snapshot_key, content_type="text/csv")
        self.upload_text_to_s3(final_csv, self.get_target_bucket(), latest_key, content_type="text/csv")

        return f"s3://{self.get_target_bucket()}/{snapshot_key}", len(final_rows)


    # =============================================================================
    # Cursor pagination helpers
    # =============================================================================

    def extract_next_cursor_from_link_header(self, link_header: Optional[str]) -> Optional[str]:
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
            self,
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
            "limit": str(DiscoveryJobConstantsEnum.PAGE_LIMIT),
        }

        if cursor:
            params["cursor"] = cursor

        response = session.get(
            DiscoveryJobConstantsEnum.HF_MODELS_URL,
            headers=headers,
            params=params,
            timeout=DiscoveryJobConstantsEnum.REQUEST_TIMEOUT_SECONDS,
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

        next_cursor = self.extract_next_cursor_from_link_header(response.headers.get("Link"))

        return payload, next_cursor, response_headers



    def retry_after_to_seconds(value: Optional[str]) -> int:
        if not value:
            return DiscoveryJobConstantsEnum.DEFAULT_429_SLEEP_SECONDS

        try:
            parsed = int(float(value))
            return max(parsed, 1)
        except ValueError:
            return DiscoveryJobConstantsEnum.DEFAULT_429_SLEEP_SECONDS


    # =============================================================================
    # Checkpoint logic
    # =============================================================================

    def load_or_create_checkpoint(self) -> Dict[str, Any]:
        existing = self.read_json_from_s3(self.get_target_bucket(), DiscoveryJobConstantsEnum.CHECKPOINT_KEY)

        if existing and existing.get("status") in {
            "RUNNING",
            "FAILED",
            "RATE_LIMITED",
            "INTERRUPTED",
        }:
            logger.info(json.dumps({
                "event": "checkpoint_found",
                "snapshot_id": existing.get("snapshot_id"),
                "next_cursor_present": bool(existing.get("next_cursor")),
                "last_successful_page": existing.get("last_successful_page"),
                "total_models_seen": existing.get("total_models_seen"),
                "models_with_emissions": existing.get("models_with_emissions"),
            }, ensure_ascii=False))
            return existing

        snapshot_id = self.utc_now_compact()

        checkpoint = {
            "snapshot_id": snapshot_id,
            "status": "RUNNING",
            "next_cursor": None,
            "last_successful_page": 0,
            "total_models_seen": 0,
            "models_with_emissions": 0,
            "part_number": 0,
            "started_at": self.utc_now_iso(),
            "updated_at": self.utc_now_iso(),
        }

        self.save_checkpoint(checkpoint)

        logger.info(json.dumps({
            "event": "checkpoint_created",
            "snapshot_id": snapshot_id,
        }, ensure_ascii=False))

        return checkpoint


    def save_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        checkpoint["updated_at"] = self.utc_now_iso()

        self.upload_json_to_s3(checkpoint, self.get_target_bucket(), DiscoveryJobConstantsEnum.CHECKPOINT_KEY)

        snapshot_id = checkpoint["snapshot_id"]
        self.upload_json_to_s3(
            checkpoint,
            self.get_target_bucket(),
            f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={snapshot_id}/_metadata/checkpoint.json",
        )


    def save_progress_event(self, snapshot_id: str, event_name: str, payload: Dict[str, Any]) -> None:
        key = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={snapshot_id}/_metadata/{event_name}.json"
        self.upload_json_to_s3(payload, self.get_target_bucket(), key)


    # =============================================================================
    # Main discovery — punto de entrada para el Glue job
    # =============================================================================

    def get_hf_token_secret_name(self):
        return env_service.load_env_variable("CUSTOMER_HF_TOKEN_SECRET_NAME")


    def run_discovery(self) -> Dict[str, Any]:
        hf_token = self.get_hf_token_from_secrets_manager(self.get_hf_token_secret_name())

        checkpoint = self.load_or_create_checkpoint()

        snapshot_id = checkpoint["snapshot_id"]
        discovered_at = checkpoint.get("started_at") or self.utc_now_iso()

        next_cursor = checkpoint.get("next_cursor")
        page_number = int(checkpoint.get("last_successful_page") or 0)
        total_models_seen = int(checkpoint.get("total_models_seen") or 0)
        models_with_emissions = int(checkpoint.get("models_with_emissions") or 0)
        part_number = int(checkpoint.get("part_number") or 0)

        pending_rows: List[Dict[str, Any]] = []
        rate_limit_retries = 0

        session = requests.Session()

        logger.info(json.dumps({
            "event": "discovery_resumed_or_started",
            "snapshot_id": snapshot_id,
            "starting_page": page_number + 1,
            "total_models_seen": total_models_seen,
            "models_with_emissions": models_with_emissions,
            "has_cursor": bool(next_cursor),
        }, ensure_ascii=False))

        try:
            while True:
                try:
                    models, new_next_cursor, response_headers = self.fetch_models_page(
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
                    self.save_checkpoint(checkpoint)

                    if pending_rows:
                        part_number += 1
                        part_uri = self.upload_rows_part(snapshot_id, part_number, pending_rows)
                        pending_rows = []
                        checkpoint["part_number"] = part_number
                        checkpoint["last_part_uri"] = part_uri
                        self.save_checkpoint(checkpoint)

                    self.save_progress_event(
                        snapshot_id=snapshot_id,
                        event_name="rate_limited",
                        payload={
                            "snapshot_id": snapshot_id,
                            "status": "RATE_LIMITED",
                            "retry_after": exc.retry_after,
                            "sleep_seconds": self.retry_after_to_seconds(exc.retry_after),
                            "total_models_seen": total_models_seen,
                            "models_with_emissions": models_with_emissions,
                            "page_number": page_number,
                            "cursor_saved": bool(next_cursor),
                            "event_at": self.utc_now_iso(),
                        },
                    )

                    rate_limit_retries += 1

                    if rate_limit_retries > DiscoveryJobConstantsEnum.MAX_429_RETRIES_PER_RUN:
                        checkpoint["status"] = "INTERRUPTED"
                        checkpoint["last_error"] = (
                            f"Exceeded MAX_429_RETRIES_PER_RUN={DiscoveryJobConstantsEnum.MAX_429_RETRIES_PER_RUN}"
                        )
                        self.save_checkpoint(checkpoint)
                        raise

                    sleep_seconds = self.retry_after_to_seconds(exc.retry_after)

                    logger.warning(json.dumps({
                        "event": "rate_limited_sleeping",
                        "sleep_seconds": sleep_seconds,
                        "retry": rate_limit_retries,
                        "max_retries": DiscoveryJobConstantsEnum.MAX_429_RETRIES_PER_RUN,
                        "snapshot_id": snapshot_id,
                    }, ensure_ascii=False))

                    time.sleep(sleep_seconds)
                    continue

                if not models:
                    logger.info(json.dumps({
                        "event": "empty_page_received",
                        "snapshot_id": snapshot_id,
                        "page_number": page_number + 1,
                    }, ensure_ascii=False))
                    break

                page_matches = 0

                for model in models:
                    total_models_seen += 1
                    row = self.model_to_row(model=model, snapshot_id=snapshot_id, discovered_at=discovered_at)
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

                if page_number % 10 == 0 or not next_cursor:
                    self.save_checkpoint(checkpoint)

                should_flush_by_pages = page_number % DiscoveryJobConstantsEnum.FLUSH_EVERY_PAGES == 0
                should_flush_by_matches = len(pending_rows) >= DiscoveryJobConstantsEnum.FLUSH_EVERY_MATCHES

                if pending_rows and (should_flush_by_pages or should_flush_by_matches):
                    part_number += 1
                    part_uri = self.upload_rows_part(snapshot_id, part_number, pending_rows)
                    pending_rows = []
                    checkpoint["part_number"] = part_number
                    checkpoint["last_part_uri"] = part_uri
                    self.save_checkpoint(checkpoint)

                    logger.info(json.dumps({
                        "event": "part_flushed",
                        "snapshot_id": snapshot_id,
                        "part_number": part_number,
                        "part_uri": part_uri,
                        "page_number": page_number,
                        "total_models_seen": total_models_seen,
                        "models_with_emissions": models_with_emissions,
                    }, ensure_ascii=False))

                logger.info(json.dumps({
                    "event": "page_processed",
                    "snapshot_id": snapshot_id,
                    "page_number": page_number,
                    "page_size": len(models),
                    "page_matches": page_matches,
                    "total_models_seen": total_models_seen,
                    "models_with_emissions": models_with_emissions,
                    "has_next_cursor": bool(next_cursor),
                }, ensure_ascii=False))

                if not next_cursor:
                    break

            if pending_rows:
                part_number += 1
                part_uri = self.upload_rows_part(snapshot_id, part_number, pending_rows)
                pending_rows = []
                checkpoint["part_number"] = part_number
                checkpoint["last_part_uri"] = part_uri
                self.save_checkpoint(checkpoint)

            snapshot_path, final_rows_count = self.consolidate_parts(snapshot_id)

            result = {
                "snapshot_id": snapshot_id,
                "status": "COMPLETED",
                "snapshot_path": snapshot_path,
                "latest_path": f"s3://{self.get_target_bucket()}/{DiscoveryJobConstantsEnum.BASE_PREFIX}/latest/models_with_emissions.csv",
                "total_models_seen": total_models_seen,
                "models_with_emissions": models_with_emissions,
                "final_rows_count": final_rows_count,
                "part_number": part_number,
                "completed_at": self.utc_now_iso(),
            }

            checkpoint["status"] = "COMPLETED"
            checkpoint["next_cursor"] = None
            checkpoint["last_successful_page"] = page_number
            checkpoint["total_models_seen"] = total_models_seen
            checkpoint["models_with_emissions"] = models_with_emissions
            checkpoint["part_number"] = part_number
            checkpoint["result"] = result
            self.save_checkpoint(checkpoint)

            self.save_progress_event(snapshot_id=snapshot_id, event_name="result", payload=result)

            logger.info(json.dumps(result, ensure_ascii=False))
            return result

        except Exception as exc:
            if pending_rows:
                part_number += 1
                part_uri = self.upload_rows_part(snapshot_id, part_number, pending_rows)
                checkpoint["part_number"] = part_number
                checkpoint["last_part_uri"] = part_uri

            checkpoint["status"] = "FAILED"
            checkpoint["next_cursor"] = next_cursor
            checkpoint["last_successful_page"] = page_number
            checkpoint["total_models_seen"] = total_models_seen
            checkpoint["models_with_emissions"] = models_with_emissions
            checkpoint["last_error"] = str(exc)
            checkpoint["failed_at"] = self.utc_now_iso()
            self.save_checkpoint(checkpoint)

            self.save_progress_event(
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
                    "failed_at": self.utc_now_iso(),
                },
            )

            logger.exception(json.dumps({
                "event": "discovery_failed",
                "snapshot_id": snapshot_id,
                "error": str(exc),
            }, ensure_ascii=False))

            raise
