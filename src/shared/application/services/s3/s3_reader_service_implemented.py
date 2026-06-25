from botocore.client import BaseClient
from typing import Any

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
