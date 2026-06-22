from application.data_preparation.services.config.environment.env_variables_service_implemented import EnvironmentVariablesService
from typing import Any

from domain.data_preparation.models.data.input_manifest import InputManifest
from domain.data_preparation.services.config.lambda_function.lambda_config_service import LambdaConfigService
from domain.data_preparation.services.config.lambda_function.lambda_config_validator_service import LambdaConfigValidatorService
from domain.data_preparation.services.date_parsing_service import DataParsingService
from application.data_preparation.services.config.lambda_function.data_preparation_enum import DataPreparationConfigParams

env_service = EnvironmentVariablesService()

class LambdaConfigServiceImplemented(LambdaConfigService):
    def __init__(self, data_parsing_service: DataParsingService, lambda_config_validator: LambdaConfigValidatorService):
        self.data_parsing_service = data_parsing_service
        self.lambda_config_validator = lambda_config_validator

    def load_input_manifest(self, event: dict[str, Any]) -> InputManifest:
        manifest = self.create_manifest_structure(event)
        self.lambda_config_validator.validate_input(manifest)
        return manifest

    def create_manifest_structure(self, event: dict[str, Any]) -> InputManifest:
        bucket_name = env_service.load_env_variable("RAW_BUCKET_NAME")
        table_name = env_service.load_env_variable("CONTROL_TABLE_NAME")
        if not table_name:
            raise ValueError("CONTROL_TABLE_NAME environment variable is required")
        run_id = event.get("run_id", self.data_parsing_service.utc_now_compact())
        return InputManifest({
            "run_id": run_id,
            "source_csv_path": event.get(
                "source_csv_path",
                f"s3://{bucket_name}/{DataPreparationConfigParams.DISCOVERY_DEFAULT_KEY.value}",
            ),
            "workers": int(event.get("workers", 4)),
            "threads_per_worker": int(event.get("threads_per_worker", 1)),
            "bucket_name": bucket_name,
            "control_table_name": table_name,
            "prepared_prefix": event.get(
                "prepared_prefix",
                f"{DataPreparationConfigParams.PREPARED_PREFIX.value}/run_id={run_id}",
            ),
            "global_rate_limit": int(
                event.get("global_rate_limit", DataPreparationConfigParams.GLOBAL_RATE_LIMIT.value)),
            "window_seconds": int(event.get("window_seconds", DataPreparationConfigParams.WINDOW_SECONDS.value)),
            "calls_per_model": int(event.get("calls_per_model", DataPreparationConfigParams.CALLS_PER_MODEL.value)),
        })