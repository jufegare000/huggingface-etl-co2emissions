import csv
import io
import json
from typing import Any, Dict, List

import boto3


class IngestionS3WriterImplemented:

    def __init__(self) -> None:
        self._s3 = boto3.client("s3")

    def write_batch(self, rows: List[Dict[str, Any]], bucket: str, key: str) -> None:
        self._put_jsonl(rows, bucket, key)

    def write_errors(self, rows: List[Dict[str, Any]], bucket: str, key: str) -> None:
        self._put_jsonl(rows, bucket, key)

    def write_metrics(self, payload: Dict[str, Any], bucket: str, key: str) -> None:
        self._put_json(payload, bucket, key)

    def write_success_marker(self, payload: Dict[str, Any], bucket: str, key: str) -> None:
        self._put_json(payload, bucket, key)

    def read_partition_csv(self, bucket: str, key: str) -> List[Dict[str, Any]]:
        response = self._s3.get_object(Bucket=bucket, Key=key)
        text = response["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))

        if not reader.fieldnames:
            raise ValueError(f"Partition CSV has no header: s3://{bucket}/{key}")

        if "model_id" not in reader.fieldnames:
            raise ValueError(f"Partition CSV must contain model_id: s3://{bucket}/{key}")

        return list(reader)

    def _put_jsonl(self, rows: List[Dict[str, Any]], bucket: str, key: str) -> None:
        body = "\n".join(
            json.dumps(row, ensure_ascii=False, default=str) for row in rows
        )
        if body:
            body += "\n"
        self._s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )

    def _put_json(self, payload: Dict[str, Any], bucket: str, key: str) -> None:
        self._s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(payload, ensure_ascii=False, indent=2, default=str).encode("utf-8"),
            ContentType="application/json",
        )
