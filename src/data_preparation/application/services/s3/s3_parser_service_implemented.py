from data_preparation.domain.models.s3.bucket_uri import BucketURI
from data_preparation.domain.services.s3.s3_parser_service import S3ParserService
from urllib.parse import urlparse


class S3ParserServiceImplemented(S3ParserService):
    S3_PARSED_SCHEMA: str = "s3"

    def parse_s3_uri(self, uri: str) -> BucketURI:
        parsed = urlparse(uri)

        if parsed.scheme != self.S3_PARSED_SCHEMA:
            raise ValueError(f"Expected s3 URI, got: {uri}")

        return BucketURI(parsed.netloc, parsed.path.lstrip("/"))
