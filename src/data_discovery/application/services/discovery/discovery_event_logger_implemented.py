import json
import logging

from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from data_discovery.domain.models.discovery_result import DiscoveryResult
from data_discovery.domain.models.discovery_run_state import DiscoveryRunState

logger = logging.getLogger(__name__)


class DiscoveryEventLoggerImplemented:

    def log_started(self, state: DiscoveryRunState) -> None:
        logger.info(json.dumps({
            "event": "discovery_resumed_or_started",
            "snapshot_id": state.snapshot_id,
            "starting_page": state.page_number + 1,
            "total_models_seen": state.total_models_seen,
            "models_with_emissions": state.models_with_emissions,
            "has_cursor": bool(state.next_cursor),
        }, ensure_ascii=False))

    def log_empty_page(self, state: DiscoveryRunState) -> None:
        logger.info(json.dumps({
            "event": "empty_page_received",
            "snapshot_id": state.snapshot_id,
            "page_number": state.page_number + 1,
        }, ensure_ascii=False))

    def log_part_flushed(self, state: DiscoveryRunState, part_uri: str) -> None:
        logger.info(json.dumps({
            "event": "part_flushed",
            "snapshot_id": state.snapshot_id,
            "part_number": state.part_number,
            "part_uri": part_uri,
            "page_number": state.page_number,
            "total_models_seen": state.total_models_seen,
            "models_with_emissions": state.models_with_emissions,
        }, ensure_ascii=False))

    def log_page_processed(self, state: DiscoveryRunState, page_size: int, page_matches: int) -> None:
        logger.info(json.dumps({
            "event": "page_processed",
            "snapshot_id": state.snapshot_id,
            "page_number": state.page_number,
            "page_size": page_size,
            "page_matches": page_matches,
            "total_models_seen": state.total_models_seen,
            "models_with_emissions": state.models_with_emissions,
            "has_next_cursor": bool(state.next_cursor),
        }, ensure_ascii=False))

    def log_rate_limit_sleeping(self, state: DiscoveryRunState, sleep_seconds: int) -> None:
        logger.warning(json.dumps({
            "event": "rate_limited_sleeping",
            "sleep_seconds": sleep_seconds,
            "retry": state.rate_limit_retries,
            "max_retries": DiscoveryJobConstantsEnum.MAX_429_RETRIES_PER_RUN,
            "snapshot_id": state.snapshot_id,
        }, ensure_ascii=False))

    def log_completed(self, result: DiscoveryResult) -> None:
        logger.info(json.dumps(result.to_dict(), ensure_ascii=False))

    def log_failed(self, snapshot_id: str, error: str) -> None:
        logger.exception(json.dumps({
            "event": "discovery_failed",
            "snapshot_id": snapshot_id,
            "error": error,
        }, ensure_ascii=False))
