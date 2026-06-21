from typing import Protocol, List, Dict, Any

from botocore.client import BaseClient


class S3Service(Protocol):

    def get_client(self) -> BaseClient:
        ...

    def write_csv_to_s3(self, rows: List[Dict[str, Any]], bucket: str, key: str) -> None:
        ...

    def write_json_to_s3(self, payload: Dict[str, Any], bucket: str, key: str) -> None:
        ...