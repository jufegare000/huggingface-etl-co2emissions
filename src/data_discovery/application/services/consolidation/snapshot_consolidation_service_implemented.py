import csv
import io
from typing import Dict, Any, Tuple

from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from shared.domain.services.s3.s3_reader_service import S3ReaderService
from shared.domain.services.s3.s3_writer_service import S3WriterService
from shared.domain.services.uploader_process.uploader_process_service import UploaderProcessService


class SnapshotConsolidationServiceImplemented:
    def __init__(
        self,
        s3_reader_service: S3ReaderService,
        s3_writer_service: S3WriterService,
        uploader_process_service: UploaderProcessService,
        target_bucket: str,
    ) -> None:
        self.s3_reader_service = s3_reader_service
        self.s3_writer_service = s3_writer_service
        self.uploader_process_service = uploader_process_service
        self.target_bucket = target_bucket

    def consolidate_parts(self, snapshot_id: str) -> Tuple[str, int]:
        parts_prefix = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={snapshot_id}/filtered_parts/"
        part_keys = sorted(
            key for key in self.s3_reader_service.list_s3_keys(self.target_bucket, parts_prefix)
            if key.endswith(".csv")
        )

        deduped: Dict[str, Dict[str, Any]] = {}
        for key in part_keys:
            text = self.s3_reader_service.read_s3_text(self.target_bucket, key)
            reader = csv.DictReader(io.StringIO(text))
            for row in reader:
                model_id = row.get("model_id")
                if not model_id:
                    continue
                deduped[model_id] = row

        final_rows = list(deduped.values())
        final_csv = self.uploader_process_service.rows_to_csv_text(final_rows)

        snapshot_key = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={snapshot_id}/filtered/models_with_emissions.csv"
        latest_key = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/latest/models_with_emissions.csv"

        self.s3_writer_service.upload_text_to_s3(final_csv, self.target_bucket, snapshot_key, content_type="text/csv")
        self.s3_writer_service.upload_text_to_s3(final_csv, self.target_bucket, latest_key, content_type="text/csv")

        return f"s3://{self.target_bucket}/{snapshot_key}", len(final_rows)
