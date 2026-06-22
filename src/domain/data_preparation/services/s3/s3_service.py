from typing import Protocol, Any

from botocore.client import BaseClient


class S3Service(Protocol):

    def get_client(self) -> BaseClient:
        ...

    def write_csv_to_s3(self, rows: list[dict[str, Any]], bucket: str, key: str) -> None:
        ...

    def write_json_to_s3(self, payload: dict[str, Any], bucket: str, key: str) -> None:
        ...
