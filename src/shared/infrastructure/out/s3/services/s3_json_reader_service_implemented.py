import json
from typing import Dict, Any, Optional

from shared.domain.services.s3.s3_json_reader_service import S3JsonReaderService
from shared.domain.services.s3.s3_reader_service import S3ReaderService


class S3JsonReaderServiceImplemented(S3JsonReaderService):

    def __init__(self, s3_reader_service: S3ReaderService) -> None:
        self.s3_reader_service = s3_reader_service

    def read_json_from_s3(self, bucket: str, key: str) -> Optional[Dict[str, Any]]:
        if not self.s3_reader_service.s3_object_exists(bucket, key):
            return None

        response = self.s3_reader_service.get_object(bucket, key)
        body = response["Body"].read().decode("utf-8")
        return json.loads(body)
