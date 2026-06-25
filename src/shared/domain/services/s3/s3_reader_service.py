from typing import Protocol, Any


class S3ReaderService(Protocol):

    def get_client(self):
        ...

    def get_object(self, bucket: str, key: str) -> dict[str, Any]:
        ...

    def s3_object_exists(self, bucket: str, key: str) -> bool:
        ...
