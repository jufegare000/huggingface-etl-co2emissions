from typing import Protocol, Optional, Dict, Any


class S3JsonReaderService(Protocol):
    def read_json_from_s3(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        ...
