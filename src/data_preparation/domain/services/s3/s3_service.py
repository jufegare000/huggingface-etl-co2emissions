from typing import Protocol, Any


class S3ServiceDataPreparationService(Protocol):


    def write_csv_to_s3(self, rows: list[dict[str, Any]], bucket: str, key: str) -> None:
        ...

    def write_json_to_s3(self, payload: dict[str, Any], bucket: str, key: str) -> None:
        ...

    def get_object(self, bucket: str, key: str) -> dict[str, Any]:
        ...

    def s3_object_exists(self, bucket: str, key: str) -> bool:
        ...


