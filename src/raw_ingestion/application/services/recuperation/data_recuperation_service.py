import time
from collections import defaultdict
from typing import Any, Dict, List
from urllib.parse import urlparse

from raw_ingestion.application.services.enrichment.row_enrichment_service import RowEnrichmentService
from raw_ingestion.application.services.rate_limit.rate_limit_service import RateLimitService
from raw_ingestion.domain.models.recuperation_metrics import RecuperationMetrics
from raw_ingestion.domain.services.hf_model_fetcher import HfModelFetcher
from raw_ingestion.domain.services.ingestion_writer import IngestionS3Writer
from raw_ingestion.domain.services.partition_repository import PartitionRepository
from shared.domain.services.date_parsing.date_parsing_service import DataParsingService
from shared.domain.services.security.secret_obtainer import SecretObtainer

DEFAULT_BATCH_SIZE = 50
DEFAULT_CALLS_PER_WINDOW = 250
DEFAULT_WINDOW_SECONDS = 300


class DataRecuperationService:

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

    def run(self, run_id: str, source_bucket: str) -> RecuperationMetrics:
        metrics = RecuperationMetrics(run_id=run_id)
        hf_token = self.secret_obtainer.get_secret_token()

        error_files = self.ingestion_writer.list_error_files(source_bucket, run_id)
        print(f"Found {len(error_files)} error file(s) for run_id={run_id}")

        failed_by_partition: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for key in error_files:
            rows = self.ingestion_writer.read_jsonl_lines(source_bucket, key)
            metrics.errors_scanned += len(rows)
            for row in rows:
                if _is_rate_limit_error(row):
                    metrics.rate_limit_errors_found += 1
                    failed_by_partition[row["partition_id"]].append(row)

        print(
            f"Rate-limit errors: {metrics.rate_limit_errors_found} "
            f"across {len(failed_by_partition)} partition(s)"
        )

        still_failed: List[Dict[str, Any]] = []
        api_calls_in_window = 0
        window_started_at = time.time()

        for partition_id, failed_rows in failed_by_partition.items():
            metrics.partitions_processed += 1
            failed_model_ids = {str(r.get("model_id", "")).strip() for r in failed_rows}
            failed_model_ids.discard("")

            partition_config = self.partition_repository.get_config(run_id, partition_id)
            csv_bucket, csv_key = _parse_s3_uri(partition_config.input_path)
            csv_rows = self.ingestion_writer.read_partition_csv(csv_bucket, csv_key)
            original_rows = {str(r.get("model_id", "")).strip(): r for r in csv_rows}

            batch: List[Dict[str, Any]] = []
            recuperation_batch_num = 0

            for model_id in failed_model_ids:
                original_row = original_rows.get(model_id, {"model_id": model_id})

                api_calls_in_window, window_started_at = (
                    self.rate_limit_service.sleep_if_budget_reached(
                        api_calls_in_window=api_calls_in_window,
                        partition_budget=DEFAULT_CALLS_PER_WINDOW,
                        window_started_at=window_started_at,
                        window_seconds=DEFAULT_WINDOW_SECONDS,
                    )
                )

                try:
                    hf_payload = self.hf_model_fetcher.fetch_model(model_id, hf_token)
                    metrics.api_calls_count += 1
                    api_calls_in_window += 1

                    enriched_row = self.row_enrichment_service.build_enriched_row(
                        row=original_row,
                        hf_payload=hf_payload,
                        run_id=run_id,
                        partition_id=partition_id,
                        enriched_at=self.date_parsing_service.utc_now_iso(),
                    )
                    batch.append(enriched_row)
                    metrics.recuperated_count += 1

                except Exception as exc:
                    metrics.api_calls_count += 1
                    api_calls_in_window += 1
                    metrics.still_failed_count += 1
                    still_failed.append(
                        self.row_enrichment_service.build_error_row(
                            row=original_row,
                            error=exc,
                            run_id=run_id,
                            partition_id=partition_id,
                            failed_at=self.date_parsing_service.utc_now_iso(),
                        )
                    )

                if len(batch) >= DEFAULT_BATCH_SIZE:
                    recuperation_batch_num += 1
                    batch_key = _recuperation_batch_key(run_id, partition_id, recuperation_batch_num)
                    self.ingestion_writer.write_batch(batch, source_bucket, batch_key)
                    batch = []

            if batch:
                recuperation_batch_num += 1
                batch_key = _recuperation_batch_key(run_id, partition_id, recuperation_batch_num)
                self.ingestion_writer.write_batch(batch, source_bucket, batch_key)

        if still_failed:
            still_failed_key = f"enriched/hf-carbon/run_id={run_id}/recuperation/still_failed.jsonl"
            self.ingestion_writer.write_errors(still_failed, source_bucket, still_failed_key)

        metrics.completed_at = self.date_parsing_service.utc_now_iso()
        summary_key = f"enriched/hf-carbon/run_id={run_id}/recuperation/summary.json"
        self.ingestion_writer.write_metrics(metrics.to_dict(), source_bucket, summary_key)

        print(
            f"Recuperation complete: recuperated={metrics.recuperated_count}, "
            f"still_failed={metrics.still_failed_count}, "
            f"api_calls={metrics.api_calls_count}"
        )
        return metrics


def _is_rate_limit_error(row: Dict[str, Any]) -> bool:
    return "HTTP 429" in str(row.get("error_message", ""))


def _recuperation_batch_key(run_id: str, partition_id: str, batch_num: int) -> str:
    return (
        f"enriched/hf-carbon/run_id={run_id}"
        f"/partition_id={partition_id}"
        f"/batches/recuperation_{batch_num:06d}.jsonl"
    )


def _parse_s3_uri(uri: str):
    parsed = urlparse(uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected S3 URI, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")
