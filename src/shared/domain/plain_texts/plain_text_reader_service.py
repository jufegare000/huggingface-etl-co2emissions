from typing import Protocol, Any


class PlainTextReaderService(Protocol):
    def read_csv_from_s3(self, s3_uri: str) -> list[dict[str, Any]]:
        ...
