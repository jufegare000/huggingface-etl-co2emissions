from typing import Protocol, Any


class S3WriterService(Protocol):


    def write_csv_to_s3(self, rows: list[dict[str, Any]], bucket: str, key: str) -> None:
        ...

    def write_json_to_s3(self, payload: dict[str, Any], bucket: str, key: str) -> None:
        ...

    def upload_text_to_s3(self, text: str, bucket: str, key: str, content_type: str = "text/plain") -> None:
        ...



