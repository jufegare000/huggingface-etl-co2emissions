import json
import logging
from typing import Dict, Any

from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from data_discovery.domain.models.checkpoint_entity import CheckpointEntity
from shared.domain.services.date_parsing.date_parsing_service import DataParsingService
from shared.domain.services.s3.s3_json_reader_service import S3JsonReaderService
from shared.domain.services.s3.s3_writer_service import S3WriterService

logger = logging.getLogger(__name__)


class CheckPointServiceImplemented:
    def __init__(
        self,
        s3_json_reader_service: S3JsonReaderService,
        s3_writer_service: S3WriterService,
        date_parsing_service: DataParsingService,
        target_bucket: str,
    ) -> None:
        self.s3_json_reader_service = s3_json_reader_service
        self.s3_writer_service = s3_writer_service
        self.date_parsing_service = date_parsing_service
        self.target_bucket = target_bucket

    def load_or_create_checkpoint(self) -> Dict[str, Any]:
        raw = self.s3_json_reader_service.read_json_from_s3(
            self.target_bucket, DiscoveryJobConstantsEnum.CHECKPOINT_KEY
        )

        if raw:
            entity = CheckpointEntity.from_dict(raw)
            if entity.is_resumable:
                logger.info(json.dumps(entity.resume_log_payload(), ensure_ascii=False))
                return raw

        entity = CheckpointEntity.new(
            snapshot_id=self.date_parsing_service.utc_now_compact(),
            started_at=self.date_parsing_service.utc_now_iso(),
        )
        checkpoint_dict = entity.to_dict()
        self.save_checkpoint(checkpoint_dict)
        logger.info(json.dumps(entity.created_log_payload(), ensure_ascii=False))
        return checkpoint_dict

    def save_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        checkpoint["updated_at"] = self.date_parsing_service.utc_now_iso()
        snapshot_id = checkpoint["snapshot_id"]

        self.s3_writer_service.write_json_to_s3(
            checkpoint, self.target_bucket, DiscoveryJobConstantsEnum.CHECKPOINT_KEY
        )
        self.s3_writer_service.write_json_to_s3(
            checkpoint,
            self.target_bucket,
            f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={snapshot_id}/_metadata/checkpoint.json",
        )

    def save_progress_event(self, snapshot_id: str, event_name: str, payload: Dict[str, Any]) -> None:
        key = f"{DiscoveryJobConstantsEnum.BASE_PREFIX}/snapshot_id={snapshot_id}/_metadata/{event_name}.json"
        self.s3_writer_service.write_json_to_s3(payload, self.target_bucket, key)
