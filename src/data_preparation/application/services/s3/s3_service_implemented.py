from botocore.client import BaseClient
from typing import Any

from botocore.exceptions import ClientError

from data_preparation.domain.services.s3.s3_service import S3ServiceDataPreparationService
import boto3
import io
import csv
import json

from shared.domain.models.datasets.bronze_datasets_columns import CSV_BROZE_COLUMNS


class S3ServiceDataPreparationServiceImplemented(S3ServiceDataPreparationService):
    s3_client: BaseClient = boto3.client("s3")


    def get_client(self) -> BaseClient:
        return self.s3_client

    def write_csv_to_s3(self, rows: list[dict[str, Any]], bucket: str, key: str) -> None:
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=CSV_BROZE_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

        self.get_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue().encode("utf-8"),
            ContentType="text/csv",
        )

    def write_json_to_s3(self, payload: dict[str, Any], bucket: str, key: str) -> None:
        self.get_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )

    def get_object(self, bucket: str, key: str) -> dict[str, Any]:
        return self.get_client().get_object(Bucket=bucket, Key=key)

    def s3_object_exists(self, bucket: str, key: str) -> bool:
        try:
            self.get_client().head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code")
            if code in ("404", "NoSuchKey", "NotFound"):
                return False
            raise
