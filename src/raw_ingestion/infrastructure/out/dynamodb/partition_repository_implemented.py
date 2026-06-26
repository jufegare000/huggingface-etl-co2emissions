from decimal import Decimal
from typing import Any

import boto3

from raw_ingestion.domain.models.ingestion_metrics import IngestionMetrics
from raw_ingestion.domain.models.partition_config import PartitionConfig, RateBudget


class PartitionRepositoryImplemented:

    def __init__(self, table_name: str) -> None:
        self._table_name = table_name
        self._dynamodb = boto3.resource("dynamodb")

    def get_config(self, run_id: str, partition_id: str) -> PartitionConfig:
        table = self._dynamodb.Table(self._table_name)
        response = table.get_item(
            Key={"PK": f"RUN#{run_id}", "SK": f"PARTITION#{partition_id}"}
        )
        item = response.get("Item")
        if not item:
            raise ValueError(
                f"Partition config not found: run_id={run_id}, partition_id={partition_id}"
            )

        rate_budget = None
        if "rate_budget" in item:
            rb = item["rate_budget"]
            rate_budget = RateBudget(
                partition_call_budget=int(rb.get("partition_call_budget", 250)),
                window_seconds=int(rb.get("window_seconds", 300)),
            )

        return PartitionConfig(
            run_id=run_id,
            partition_id=partition_id,
            input_path=item["input_path"],
            records_count=int(item.get("records_count", 0)),
            rate_budget=rate_budget,
        )

    def mark_running(self, run_id: str, partition_id: str, job_name: str) -> None:
        from botocore.exceptions import ClientError
        from shared.application.services.date_parsing.date_parsing_service_implemented import (
            SystemDateTimeServiceImplemented,
        )

        from raw_ingestion.domain.models.exceptions import PartitionAlreadyCompletedException

        now = SystemDateTimeServiceImplemented().utc_now_iso()
        table = self._dynamodb.Table(self._table_name)
        try:
            table.update_item(
                Key={"PK": f"RUN#{run_id}", "SK": f"PARTITION#{partition_id}"},
                UpdateExpression="""
                    SET #status = :running,
                        updated_at = :updated_at,
                        started_at = if_not_exists(started_at, :started_at),
                        glue_job_name = :job_name
                    ADD attempts :one
                """,
                ConditionExpression="#status = :pending OR #status = :failed OR #status = :running",
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues=_to_decimal({
                    ":running": "RUNNING",
                    ":pending": "PENDING",
                    ":failed": "FAILED",
                    ":updated_at": now,
                    ":started_at": now,
                    ":job_name": job_name,
                    ":one": 1,
                }),
            )
        except ClientError as exc:
            if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise
            response = table.get_item(
                Key={"PK": f"RUN#{run_id}", "SK": f"PARTITION#{partition_id}"},
                ProjectionExpression="#s",
                ExpressionAttributeNames={"#s": "status"},
            )
            current_status = response.get("Item", {}).get("status", "UNKNOWN")
            if current_status == "COMPLETED":
                raise PartitionAlreadyCompletedException(
                    f"Partition {partition_id} in run {run_id} is already COMPLETED"
                ) from exc
            raise RuntimeError(
                f"Cannot mark partition {partition_id} as RUNNING: current status is {current_status!r}"
            ) from exc

    def update_progress(self, run_id: str, partition_id: str, metrics: IngestionMetrics) -> None:
        from shared.application.services.date_parsing.date_parsing_service_implemented import (
            SystemDateTimeServiceImplemented,
        )

        now = SystemDateTimeServiceImplemented().utc_now_iso()
        table = self._dynamodb.Table(self._table_name)
        table.update_item(
            Key={"PK": f"RUN#{run_id}", "SK": f"PARTITION#{partition_id}"},
            UpdateExpression="""
                SET processed_count = :processed_count,
                    success_count = :success_count,
                    failed_count = :failed_count,
                    api_calls_count = :api_calls_count,
                    batches_written = :batches_written,
                    last_batch_path = :last_batch_path,
                    updated_at = :updated_at
            """,
            ExpressionAttributeValues=_to_decimal({
                ":processed_count": metrics.processed_count,
                ":success_count": metrics.success_count,
                ":failed_count": metrics.failed_count,
                ":api_calls_count": metrics.api_calls_count,
                ":batches_written": metrics.batches_written,
                ":last_batch_path": metrics.last_batch_path,
                ":updated_at": now,
            }),
        )

    def mark_completed(
        self,
        run_id: str,
        partition_id: str,
        output_prefix: str,
        success_marker_path: str,
        metrics: IngestionMetrics,
    ) -> None:
        from shared.application.services.date_parsing.date_parsing_service_implemented import (
            SystemDateTimeServiceImplemented,
        )

        now = SystemDateTimeServiceImplemented().utc_now_iso()
        table = self._dynamodb.Table(self._table_name)
        table.update_item(
            Key={"PK": f"RUN#{run_id}", "SK": f"PARTITION#{partition_id}"},
            UpdateExpression="""
                SET #status = :completed,
                    output_prefix = :output_prefix,
                    success_marker_path = :success_marker_path,
                    #metrics.input_count = :input_count,
                    #metrics.processed_count = :processed_count,
                    #metrics.success_count = :success_count,
                    #metrics.failed_count = :failed_count,
                    #metrics.api_calls_count = :api_calls_count,
                    #metrics.batches_written = :batches_written,
                    updated_at = :updated_at,
                    completed_at = :completed_at
            """,
            ExpressionAttributeNames={"#status": "status", "#metrics": "metrics"},
            ExpressionAttributeValues=_to_decimal({
                ":completed": "COMPLETED",
                ":output_prefix": output_prefix,
                ":success_marker_path": success_marker_path,
                ":input_count": metrics.input_count,
                ":processed_count": metrics.success_count + metrics.failed_count,
                ":success_count": metrics.success_count,
                ":failed_count": metrics.failed_count,
                ":api_calls_count": metrics.api_calls_count,
                ":batches_written": metrics.batches_written,
                ":updated_at": now,
                ":completed_at": now,
            }),
        )

    def mark_failed(
        self,
        run_id: str,
        partition_id: str,
        error: Exception,
        metrics: IngestionMetrics,
    ) -> None:
        from shared.application.services.date_parsing.date_parsing_service_implemented import (
            SystemDateTimeServiceImplemented,
        )

        now = SystemDateTimeServiceImplemented().utc_now_iso()
        table = self._dynamodb.Table(self._table_name)
        try:
            table.update_item(
                Key={"PK": f"RUN#{run_id}", "SK": f"PARTITION#{partition_id}"},
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
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues=_to_decimal({
                    ":failed": "FAILED",
                    ":processed_count": metrics.processed_count,
                    ":success_count": metrics.success_count,
                    ":failed_count": metrics.failed_count,
                    ":api_calls_count": metrics.api_calls_count,
                    ":error_type": type(error).__name__,
                    ":error_message": str(error),
                    ":updated_at": now,
                }),
            )
        except Exception:
            pass


def _to_decimal(value: Any) -> Any:
    if isinstance(value, float):
        return Decimal(str(value))
    if isinstance(value, dict):
        return {k: _to_decimal(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_to_decimal(v) for v in value]
    return value
