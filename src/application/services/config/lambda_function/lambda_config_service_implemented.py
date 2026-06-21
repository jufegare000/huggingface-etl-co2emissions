from application.services.config.env_variables_service_implemented import EnvironmentVariablesService
from domain.extract.services.config.lambda_config_service import LambdaConfigService
from typing import Dict, Any

from domain.extract.services.date_parsing_service import DataParsingService
from application.services.config.lambda_function.data_preparation_enum import DataPreparationConfigParams


class LambdaConfigServiceImplemented(LambdaConfigService):

    def __init__(self, data_parsing_service: DataParsingService):
        self.data_parsing_service = data_parsing_service

    def load_input_manifest(self, event: Dict[str, Any]) -> Dict[str, Any]:
        bucket_name = EnvironmentVariablesService.load_env_variable("RAW_BUCKET_NAME")
        table_name = EnvironmentVariablesService.load_env_variable("CONTROL_TABLE_NAME")

        if not table_name:
            raise ValueError("CONTROL_TABLE_NAME environment variable is required")

        run_id = event.get("run_id", self.data_parsing_service.utc_now_compact())

        return {
            "run_id": run_id,
            "source_csv_path": event.get(
                "source_csv_path",
                f"s3://{bucket_name}/{DataPreparationConfigParams.DISCOVERY_DEFAULT_KEY}",
            ),
            "workers": int(event.get("workers", 4)),
            "threads_per_worker": int(event.get("threads_per_worker", 1)),
            "bucket_name": bucket_name,
            "control_table_name": table_name,
            "prepared_prefix": event.get(
                "prepared_prefix",
                f"{DataPreparationConfigParams.PREPARED_PREFIX}/run_id={run_id}",
            ),
            "global_rate_limit": int(
                event.get("global_rate_limit", DataPreparationConfigParams.GLOBAL_RATE_LIMIT.value)),
            "window_seconds": int(event.get("window_seconds", DataPreparationConfigParams.WINDOW_SECONDS.value)),
            "calls_per_model": int(event.get("calls_per_model", DataPreparationConfigParams.CALLS_PER_MODEL.value)),
        }
