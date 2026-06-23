from botocore.client import BaseClient
from typing import Any

from application.data_preparation.services.s3.raw_data_set_columns import CSV_COLUMNS
from domain.data_preparation.services.s3.s3_service import S3Service
import boto3
import io
import csv
import json

class S3ServiceImplemented(S3Service):
    s3_client: BaseClient = boto3.client("s3")

    def get_client(self) -> BaseClient:
        return self.s3_client

    def write_csv_to_s3(self, rows: list[dict[str, Any]], bucket: str, key: str) -> None:
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=CSV_COLUMNS, extrasaction="ignore")
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