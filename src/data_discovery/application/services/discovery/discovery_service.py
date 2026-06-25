from typing import Any, Dict

from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from data_discovery.domain.models.discovery_result import DiscoveryResult
from data_discovery.domain.models.discovery_run_state import DiscoveryRunState
from data_discovery.domain.services.checkpoint.checkpoint_service import CheckpointService
from data_discovery.domain.services.consolidation.snapshot_consolidation_service import SnapshotConsolidationService
from data_discovery.domain.services.discovery.discovery_event_logger import DiscoveryEventLogger
from data_discovery.domain.services.discovery.part_flusher_service import PartFlusherService
from data_discovery.domain.services.discovery.rate_limit_handler_service import RateLimitHandlerService
from data_discovery.domain.services.hugging_face.hf_models_api_client import HfModelsApiClient
from shared.domain.exceptions.rate_limit_error import RateLimitError
from shared.domain.services.date_parsing.date_parsing_service import DataParsingService
from shared.domain.services.hugging_face.hugging_face_type_helpers import HuggingFaceTypesHelpers
from shared.domain.services.security.secret_obtainer import SecretObtainer


class DiscoveryJobService:

    def __init__(
        self,
        date_parsing_service: DataParsingService,
        secrets_obtainer: SecretObtainer,
        hugging_face_types_helpers: HuggingFaceTypesHelpers,
        hf_api_client: HfModelsApiClient,
        checkpoint_service: CheckpointService,
        consolidation_service: SnapshotConsolidationService,
        part_flusher: PartFlusherService,
        rate_limit_handler: RateLimitHandlerService,
        event_logger: DiscoveryEventLogger,
        target_bucket: str,
    ):
        self.date_parsing_service = date_parsing_service
        self.secrets_obtainer = secrets_obtainer
        self.hugging_face_types_helpers = hugging_face_types_helpers
        self.hf_api_client = hf_api_client
        self.checkpoint_service = checkpoint_service
        self.consolidation_service = consolidation_service
        self.part_flusher = part_flusher
        self.rate_limit_handler = rate_limit_handler
        self.event_logger = event_logger
        self.target_bucket = target_bucket

    def run_discovery(self) -> Dict[str, Any]:
        hf_token = self.secrets_obtainer.get_secret_token()
        checkpoint = self.checkpoint_service.load_or_create_checkpoint()
        state = DiscoveryRunState.from_checkpoint(checkpoint, self.date_parsing_service.utc_now_iso())

        self.event_logger.log_started(state)

        try:
            while True:
                try:
                    models, next_cursor, headers = self.hf_api_client.fetch_models_page(
                        hf_token=hf_token, cursor=state.next_cursor
                    )
                    state.rate_limit_retries = 0
                except RateLimitError as exc:
                    self.rate_limit_handler.handle(exc, state, checkpoint)
                    continue

                if not models:
                    self.event_logger.log_empty_page(state)
                    break

                matched_rows, page_matches = self.hugging_face_types_helpers.filter_page(
                    models, state.snapshot_id, state.discovered_at
                )
                state.total_models_seen += len(models)
                state.pending_rows.extend(matched_rows)
                state.models_with_emissions += page_matches
                state.page_number += 1
                state.next_cursor = next_cursor

                state.apply_to_checkpoint(checkpoint)
                checkpoint["status"] = "RUNNING"
                checkpoint["last_rate_limit"] = headers.get("RateLimit")
                checkpoint["last_rate_limit_policy"] = headers.get("RateLimit-Policy")

                if state.page_number % 10 == 0 or not state.next_cursor:
                    self.checkpoint_service.save_checkpoint(checkpoint)

                self.part_flusher.flush_on_condition(state, checkpoint)
                self.event_logger.log_page_processed(state, len(models), page_matches)

                if not state.next_cursor:
                    break

            self.part_flusher.flush_if_pending(state, checkpoint)
            snapshot_path, final_rows_count = self.consolidation_service.consolidate_parts(state.snapshot_id)

            latest_path = f"s3://{self.target_bucket}/{DiscoveryJobConstantsEnum.BASE_PREFIX}/latest/models_with_emissions.csv"
            result = DiscoveryResult.from_state(state, snapshot_path, final_rows_count, latest_path, self.date_parsing_service.utc_now_iso())

            checkpoint["status"] = "COMPLETED"
            checkpoint["next_cursor"] = None
            state.apply_to_checkpoint(checkpoint)
            checkpoint["result"] = result.to_dict()
            self.checkpoint_service.save_checkpoint(checkpoint)
            self.checkpoint_service.save_progress_event(state.snapshot_id, "result", result.to_dict())

            self.event_logger.log_completed(result)
            return result.to_dict()

        except Exception as exc:
            self.part_flusher.flush_if_pending(state, checkpoint)
            checkpoint["status"] = "FAILED"
            state.apply_to_checkpoint(checkpoint)
            checkpoint["last_error"] = str(exc)
            checkpoint["failed_at"] = self.date_parsing_service.utc_now_iso()
            self.checkpoint_service.save_checkpoint(checkpoint)
            self.checkpoint_service.save_progress_event(
                state.snapshot_id, "error",
                state.to_error_payload(str(exc), self.date_parsing_service.utc_now_iso()),
            )
            self.event_logger.log_failed(state.snapshot_id, str(exc))
            raise
