from typing import Any, Dict, List, Optional, Set
from urllib.parse import urlparse

from raw_ingestion.domain.models.partition_config import RateBudget
from raw_ingestion.domain.services.ingestion_writer import IngestionS3Writer
from raw_ingestion.domain.services.partition_repository import PartitionRepository

RETRY_PARTITION_SIZE = 250
RETRY_RATE_BUDGET = RateBudget(partition_call_budget=200, window_seconds=300)
RETRY_PARTITION_PREFIX = "r-"


class ErrorRedistributionService:

    def __init__(
        self,
        ingestion_writer: IngestionS3Writer,
        partition_repository: PartitionRepository,
    ) -> None:
        self.ingestion_writer = ingestion_writer
        self.partition_repository = partition_repository

    def run(
        self,
        run_id: str,
        bucket: str,
        partition_size: int = RETRY_PARTITION_SIZE,
    ) -> List[str]:
        error_keys = self.ingestion_writer.list_error_files(bucket, run_id)

        errors_by_partition: Dict[str, List[str]] = {}
        for key in error_keys:
            rows = self.ingestion_writer.read_jsonl_lines(bucket, key)
            for row in rows:
                if _is_retriable(row):
                    partition_id = row.get("partition_id", "")
                    model_id = str(row.get("model_id", "")).strip()
                    if model_id and partition_id:
                        errors_by_partition.setdefault(partition_id, []).append(model_id)

        if not errors_by_partition:
            return []

        retriable_rows = self._collect_original_rows(run_id, errors_by_partition)
        if not retriable_rows:
            return []

        return self._write_retry_partitions(run_id, bucket, retriable_rows, partition_size)

    def _collect_original_rows(
        self,
        run_id: str,
        errors_by_partition: Dict[str, List[str]],
    ) -> List[Dict[str, Any]]:
        retriable_rows: List[Dict[str, Any]] = []
        seen: Set[str] = set()

        for partition_id, failed_model_ids in errors_by_partition.items():
            failed_set = set(failed_model_ids)
            config = self.partition_repository.get_config(run_id, partition_id)
            orig_bucket = _parse_bucket(config.input_path)
            orig_key = _parse_key(config.input_path)
            for row in self.ingestion_writer.read_partition_csv(orig_bucket, orig_key):
                model_id = str(row.get("model_id", "")).strip()
                if model_id in failed_set and model_id not in seen:
                    retriable_rows.append(row)
                    seen.add(model_id)

        return retriable_rows

    def _write_retry_partitions(
        self,
        run_id: str,
        bucket: str,
        rows: List[Dict[str, Any]],
        partition_size: int,
    ) -> List[str]:
        chunks = [rows[i: i + partition_size] for i in range(0, len(rows), partition_size)]
        retry_ids: List[str] = []

        for idx, chunk in enumerate(chunks):
            retry_partition_id = f"{RETRY_PARTITION_PREFIX}{idx:06d}"
            csv_key = f"raw-ingestion/{run_id}/retry/{retry_partition_id}/input.csv"
            input_path = f"s3://{bucket}/{csv_key}"

            self.ingestion_writer.write_partition_csv(chunk, bucket, csv_key)
            self.partition_repository.register_partition(
                run_id=run_id,
                partition_id=retry_partition_id,
                input_path=input_path,
                records_count=len(chunk),
                rate_budget=RETRY_RATE_BUDGET,
            )
            retry_ids.append(retry_partition_id)
            print(f"Registered retry partition {retry_partition_id} with {len(chunk)} rows")

        return retry_ids


def _is_retriable(row: Dict[str, Any]) -> bool:
    error_type = row.get("error_type", "")
    error_message = row.get("error_message", "")
    return (
        error_type == "RateLimitError"
        or "429" in error_message
        or "rate limit" in error_message.lower()
    )


def _parse_bucket(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected S3 URI, got: {uri}")
    return parsed.netloc


def _parse_key(uri: str) -> str:
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected S3 URI, got: {uri}")
    return parsed.path.lstrip("/")
