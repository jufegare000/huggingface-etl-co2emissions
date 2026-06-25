from botocore.client import BaseClient
from typing import Any

import boto3
import io
import csv
import json

from shared.domain.models.datasets.bronze_datasets_columns import CSV_BROZE_COLUMNS
from shared.domain.services.s3.s3_writer_service import S3WriterService


class S3WriterServiceImplemented(S3WriterService):
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

    def upload_text_to_s3(self, text: str, bucket: str, key: str, content_type: str = "text/plain") -> None:
        self.get_client().put_object(
            Bucket=bucket,
            Key=key,
            Body=text.encode("utf-8"),
            ContentType=content_type,
        )

