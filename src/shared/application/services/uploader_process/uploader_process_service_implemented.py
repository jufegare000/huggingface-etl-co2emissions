from typing import List, Dict, Any, Optional
import csv
import io
from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from shared.domain.models.datasets.bronze_datasets_columns import CSV_BROZE_COLUMNS
from shared.domain.services.s3.s3_writer_service import S3WriterService
from shared.domain.services.uploader_process.uploader_process_service import UploaderProcessService


class UploaderProcessServiceImplemented(UploaderProcessService):
    def __init__(self, s3_writer_service: S3WriterService, target_bucket: str) -> None:
        self.s3_writer_service = s3_writer_service
        self.target_bucket = target_bucket

    def upload_rows_part(
            self,
            snapshot_id: str,
            part_number: int,
            rows: List[Dict[str, Any]],
    ) -> Optional[str]:
        if not rows:
            return None

        key = (
            f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={snapshot_id}/filtered_parts/"
            f"models_with_emissions_part_{part_number:06d}.csv"
        )

        csv_text = self.rows_to_csv_text(rows)
        self.s3_writer_service.upload_text_to_s3(csv_text, self.target_bucket, key, content_type="text/csv")

        return f"s3://{self.target_bucket}/{key}"

    def rows_to_csv_text(self, rows: List[Dict[str, Any]]) -> str:
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=CSV_BROZE_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
        return buffer.getvalue()

