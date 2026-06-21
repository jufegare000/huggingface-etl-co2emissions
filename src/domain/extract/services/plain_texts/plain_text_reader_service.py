from typing import Protocol, List, Dict, Any


class PlainTextReaderService(Protocol):
    def read_csv_from_s3(self, s3_uri: str) -> List[Dict[str, Any]]:
        ...
