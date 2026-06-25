from botocore.client import BaseClient
from typing import Any, List

from botocore.exceptions import ClientError

import boto3

from shared.domain.services.s3.s3_reader_service import S3ReaderService


class S3ReaderServiceImplemented(S3ReaderService):
    s3_client: BaseClient = boto3.client("s3")


    def get_client(self) -> BaseClient:
        return self.s3_client

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

    def list_s3_keys(self, bucket: str, prefix: str) -> List[str]:
        keys: List[str] = []
        paginator = self.get_client().get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for item in page.get("Contents", []):
                keys.append(item["Key"])

        return keys

    def read_s3_text(self, bucket: str, key: str) -> str:
        response = self.get_client().get_object(Bucket=bucket, Key=key)
        return response["Body"].read().decode("utf-8")