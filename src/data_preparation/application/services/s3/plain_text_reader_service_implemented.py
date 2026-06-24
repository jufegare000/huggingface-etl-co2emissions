import csv
import io
from typing import Any
from data_preparation.domain.models.s3.bucket_uri import BucketURI
from data_preparation.domain.services.plain_texts.plain_text_reader_service import PlainTextReaderService
from data_preparation.domain.services.s3.s3_parser_service import S3ParserService
from data_preparation.domain.services.s3.s3_service import S3Service


class PlainTextReaderServiceImplemented(PlainTextReaderService):

    def __init__(self, s3_uri_service: S3ParserService, s3_service: S3Service):
        self.s3_uri_service = s3_uri_service
        self.s3_service = s3_service

    def read_csv_from_s3(self, s3_uri: str) -> list[dict[str, Any]]:
        parsed_uri: BucketURI = self.s3_uri_service.parse_s3_uri(s3_uri)

        response = self.s3_service.get_client().get_object(
            Bucket=parsed_uri.bucket,
            Key=parsed_uri.value,
        )

        text = response["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(text))

        if not reader.fieldnames:
            raise ValueError(f"CSV has no header: {s3_uri}")

        if "model_id" not in reader.fieldnames:
            raise ValueError("CSV must contain model_id")

        if "co2_eq_emissions" not in reader.fieldnames:
            raise ValueError("CSV must contain co2_eq_emissions")

        return list(reader)
