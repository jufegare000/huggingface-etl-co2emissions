from typing import Dict, Any

from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from data_discovery.domain.models.discovery_run_state import DiscoveryRunState
from data_discovery.domain.services.checkpoint.checkpoint_service import CheckpointService
from data_discovery.domain.services.discovery.discovery_event_logger import DiscoveryEventLogger
from shared.domain.services.uploader_process.uploader_process_service import UploaderProcessService


class PartFlusherServiceImplemented:
    def __init__(
        self,
        uploader_process_service: UploaderProcessService,
        checkpoint_service: CheckpointService,
        event_logger: DiscoveryEventLogger,
    ) -> None:
        self.uploader_process_service = uploader_process_service
        self.checkpoint_service = checkpoint_service
        self.event_logger = event_logger

    def flush_if_pending(self, state: DiscoveryRunState, checkpoint: Dict[str, Any]) -> None:
        if not state.pending_rows:
            return
        state.part_number += 1
        part_uri = self.uploader_process_service.upload_rows_part(
            state.snapshot_id, state.part_number, state.pending_rows
        )
        state.pending_rows = []
        state.apply_to_checkpoint(checkpoint)
        checkpoint["last_part_uri"] = part_uri
        self.checkpoint_service.save_checkpoint(checkpoint)
        self.event_logger.log_part_flushed(state, part_uri)

    def flush_on_condition(self, state: DiscoveryRunState, checkpoint: Dict[str, Any]) -> None:
        if not state.pending_rows:
            return
        should_flush = (
            state.page_number % DiscoveryJobConstantsEnum.FLUSH_EVERY_PAGES == 0
            or len(state.pending_rows) >= DiscoveryJobConstantsEnum.FLUSH_EVERY_MATCHES
        )
        if should_flush:
            self.flush_if_pending(state, checkpoint)
