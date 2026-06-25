from typing import Dict, Any

from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from data_discovery.domain.models.checkpoint_entity import CheckpointEntity
from data_discovery.domain.services.checkpoint.checkpoint_service import CheckpointService


class CheckPointServiceImplemented(CheckpointService):



    def load_or_create_checkpoint(self) -> CheckpointEntity:
        raw = self.read_json_from_s3(self.get_target_bucket(), DiscoveryJobConstantsEnum.CHECKPOINT_KEY)

        if raw:
            checkpoint = CheckpointEntity.from_dict(raw)
            if checkpoint.is_resumable:
                logger.info(json.dumps(checkpoint.resume_log_payload(), ensure_ascii=False))
                return checkpoint

        checkpoint = CheckpointEntity.new(
            snapshot_id=self.date_parsing_service.utc_now_compact(),
            started_at=self.date_parsing_service.utc_now_iso(),
        )
        self.save_checkpoint(checkpoint.to_dict())
        logger.info(json.dumps(checkpoint.created_log_payload(), ensure_ascii=False))
        return checkpoint

