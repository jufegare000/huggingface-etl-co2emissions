from typing import Protocol

from domain.extract.models.s3.bucket_uri import BucketURI


class S3ParserService(Protocol):

    def parse_s3_uri(self, uri: str) -> BucketURI:
        ...
