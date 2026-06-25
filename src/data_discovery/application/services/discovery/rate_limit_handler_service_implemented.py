import time
from typing import Dict, Any

from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from data_discovery.domain.models.discovery_run_state import DiscoveryRunState
from data_discovery.domain.services.checkpoint.checkpoint_service import CheckpointService
from data_discovery.domain.services.discovery.discovery_event_logger import DiscoveryEventLogger
from data_discovery.domain.services.discovery.part_flusher_service import PartFlusherService
from data_discovery.domain.services.hugging_face.hf_models_api_client import HfModelsApiClient
from shared.domain.exceptions.rate_limit_error import RateLimitError
from shared.domain.services.date_parsing.date_parsing_service import DataParsingService


class RateLimitHandlerServiceImplemented:
    def __init__(
        self,
        hf_api_client: HfModelsApiClient,
        checkpoint_service: CheckpointService,
        part_flusher: PartFlusherService,
        event_logger: DiscoveryEventLogger,
        date_parsing_service: DataParsingService,
    ) -> None:
        self.hf_api_client = hf_api_client
        self.checkpoint_service = checkpoint_service
        self.part_flusher = part_flusher
        self.event_logger = event_logger
        self.date_parsing_service = date_parsing_service

    def handle(
        self,
        exc: RateLimitError,
        state: DiscoveryRunState,
        checkpoint: Dict[str, Any],
    ) -> None:
        checkpoint["status"] = "RATE_LIMITED"
        checkpoint["last_error"] = str(exc)
        checkpoint["last_retry_after"] = exc.retry_after
        state.apply_to_checkpoint(checkpoint)
        self.checkpoint_service.save_checkpoint(checkpoint)

        self.part_flusher.flush_if_pending(state, checkpoint)

        sleep_seconds = self.hf_api_client.retry_after_to_seconds(exc.retry_after)

        self.checkpoint_service.save_progress_event(
            snapshot_id=state.snapshot_id,
            event_name="rate_limited",
            payload=state.to_rate_limited_payload(sleep_seconds, exc.retry_after, self.date_parsing_service.utc_now_iso()),
        )

        state.rate_limit_retries += 1

        if state.rate_limit_retries > DiscoveryJobConstantsEnum.MAX_429_RETRIES_PER_RUN:
            checkpoint["status"] = "INTERRUPTED"
            checkpoint["last_error"] = (
                f"Exceeded MAX_429_RETRIES_PER_RUN={DiscoveryJobConstantsEnum.MAX_429_RETRIES_PER_RUN}"
            )
            state.apply_to_checkpoint(checkpoint)
            self.checkpoint_service.save_checkpoint(checkpoint)
            raise exc

        self.event_logger.log_rate_limit_sleeping(state, sleep_seconds)
        time.sleep(sleep_seconds)
