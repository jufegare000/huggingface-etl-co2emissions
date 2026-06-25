from typing import Protocol, Any, List


class S3ReaderService(Protocol):

    def get_client(self):
        ...

    def get_object(self, bucket: str, key: str) -> dict[str, Any]:
        ...

    def s3_object_exists(self, bucket: str, key: str) -> bool:
        ...

    def list_s3_keys(self, bucket: str, prefix: str) -> List[str]:
        ...

    def read_s3_text(self, bucket: str, key: str) -> str:
        ...
