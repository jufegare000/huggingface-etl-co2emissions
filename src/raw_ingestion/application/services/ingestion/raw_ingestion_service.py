import time
from typing import Any, Dict, List
from urllib.parse import urlparse

from raw_ingestion.application.services.enrichment.row_enrichment_service import RowEnrichmentService
from raw_ingestion.application.services.rate_limit.rate_limit_service import RateLimitService
from raw_ingestion.domain.models.exceptions import PartitionAlreadyCompletedException
from raw_ingestion.domain.models.ingestion_metrics import IngestionMetrics
from raw_ingestion.domain.services.hf_model_fetcher import HfModelFetcher
from raw_ingestion.domain.services.ingestion_writer import IngestionS3Writer
from raw_ingestion.domain.services.partition_repository import PartitionRepository
from shared.domain.services.date_parsing.date_parsing_service import DataParsingService
from shared.domain.services.security.secret_obtainer import SecretObtainer

DEFAULT_BATCH_SIZE = 50
DEFAULT_WINDOW_SECONDS = 300
DEFAULT_CALLS_PER_WINDOW = 250


class RawIngestionService:

    def __init__(
        self,
        hf_model_fetcher: HfModelFetcher,
        partition_repository: PartitionRepository,
        ingestion_writer: IngestionS3Writer,
        row_enrichment_service: RowEnrichmentService,
        rate_limit_service: RateLimitService,
        date_parsing_service: DataParsingService,
        secret_obtainer: SecretObtainer,
    ) -> None:
        self.hf_model_fetcher = hf_model_fetcher
        self.partition_repository = partition_repository
        self.ingestion_writer = ingestion_writer
        self.row_enrichment_service = row_enrichment_service
        self.rate_limit_service = rate_limit_service
        self.date_parsing_service = date_parsing_service
        self.secret_obtainer = secret_obtainer

    def run(self, run_id: str, partition_id: str, job_name: str) -> IngestionMetrics:
        ingestion_metrics = IngestionMetrics(run_id=run_id, partition_id=partition_id)

        try:
            hf_token = self.secret_obtainer.get_secret_token()
            print(f"HF token retrieved (len={len(hf_token)})")

            partition_config = self.partition_repository.get_config(run_id, partition_id)
            try:
                self.partition_repository.mark_running(run_id, partition_id, job_name)
            except PartitionAlreadyCompletedException:
                print(f"Partition {partition_id} in run {run_id} is already COMPLETED, skipping")
                return ingestion_metrics

            bucket = _parse_bucket(partition_config.input_path)
            output_prefix = f"enriched/hf-carbon/run_id={run_id}/partition_id={partition_id}"

            rows = self.ingestion_writer.read_partition_csv(
                bucket, _parse_key(partition_config.input_path)
            )
            ingestion_metrics.input_count = len(rows)

            if partition_config.records_count and partition_config.records_count != len(rows):
                print(
                    f"Warning: records_count mismatch. "
                    f"DynamoDB={partition_config.records_count}, CSV={len(rows)}"
                )

            partition_budget = DEFAULT_CALLS_PER_WINDOW
            window_seconds = DEFAULT_WINDOW_SECONDS

            if partition_config.rate_budget:
                partition_budget = partition_config.rate_budget.partition_call_budget
                window_seconds = partition_config.rate_budget.window_seconds

            batch: List[Dict[str, Any]] = []
            errors: List[Dict[str, Any]] = []
            window_started_at = time.time()
            api_calls_in_window = 0

            for row in rows:
                model_id = str(row.get("model_id") or "").strip()

                if not model_id:
                    ingestion_metrics.failed_count += 1
                    ingestion_metrics.processed_count += 1
                    errors.append(
                        self.row_enrichment_service.build_error_row(
                            row=row,
                            error=ValueError("Missing model_id"),
                            run_id=run_id,
                            partition_id=partition_id,
                            failed_at=self.date_parsing_service.utc_now_iso(),
                        )
                    )
                    continue

                api_calls_in_window, window_started_at = (
                    self.rate_limit_service.sleep_if_budget_reached(
                        api_calls_in_window=api_calls_in_window,
                        partition_budget=partition_budget,
                        window_started_at=window_started_at,
                        window_seconds=window_seconds,
                    )
                )

                try:
                    hf_payload = self.hf_model_fetcher.fetch_model(model_id, hf_token)
                    ingestion_metrics.api_calls_count += 1
                    api_calls_in_window += 1

                    enriched_row = self.row_enrichment_service.build_enriched_row(
                        row=row,
                        hf_payload=hf_payload,
                        run_id=run_id,
                        partition_id=partition_id,
                        enriched_at=self.date_parsing_service.utc_now_iso(),
                    )
                    batch.append(enriched_row)
                    ingestion_metrics.success_count += 1

                except Exception as exc:
                    ingestion_metrics.api_calls_count += 1
                    api_calls_in_window += 1
                    errors.append(
                        self.row_enrichment_service.build_error_row(
                            row=row,
                            error=exc,
                            run_id=run_id,
                            partition_id=partition_id,
                            failed_at=self.date_parsing_service.utc_now_iso(),
                        )
                    )
                    ingestion_metrics.failed_count += 1

                ingestion_metrics.processed_count += 1

                if len(batch) >= DEFAULT_BATCH_SIZE:
                    ingestion_metrics.batches_written += 1
                    batch_key = (
                        f"{output_prefix}/batches/batch_{ingestion_metrics.batches_written:06d}.jsonl"
                    )
                    self.ingestion_writer.write_batch(batch, bucket, batch_key)
                    ingestion_metrics.last_batch_path = f"s3://{bucket}/{batch_key}"
                    batch = []

                    self.partition_repository.update_progress(run_id, partition_id, ingestion_metrics)

            if batch:
                ingestion_metrics.batches_written += 1
                batch_key = (
                    f"{output_prefix}/batches/batch_{ingestion_metrics.batches_written:06d}.jsonl"
                )
                self.ingestion_writer.write_batch(batch, bucket, batch_key)
                ingestion_metrics.last_batch_path = f"s3://{bucket}/{batch_key}"

            if errors:
                self.ingestion_writer.write_errors(
                    errors, bucket, f"{output_prefix}/errors/errors.jsonl"
                )

            ingestion_metrics.completed_at = self.date_parsing_service.utc_now_iso()
            self.ingestion_writer.write_metrics(
                ingestion_metrics.to_dict(), bucket, f"{output_prefix}/metrics/metrics.json"
            )

            success_key = f"{output_prefix}/_SUCCESS"
            success_payload = {
                "run_id": run_id,
                "partition_id": partition_id,
                "status": "COMPLETED",
                "input_count": ingestion_metrics.input_count,
                "success_count": ingestion_metrics.success_count,
                "failed_count": ingestion_metrics.failed_count,
                "api_calls_count": ingestion_metrics.api_calls_count,
                "batches_written": ingestion_metrics.batches_written,
                "success_marker_created_at": self.date_parsing_service.utc_now_iso(),
            }
            self.ingestion_writer.write_success_marker(success_payload, bucket, success_key)

            self.partition_repository.mark_completed(
                run_id=run_id,
                partition_id=partition_id,
                output_prefix=output_prefix,
                success_marker_path=f"s3://{bucket}/{success_key}",
                metrics=ingestion_metrics,
            )

        except Exception as exc:
            self.partition_repository.mark_failed(run_id, partition_id, exc, ingestion_metrics)
            raise

        return ingestion_metrics


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
