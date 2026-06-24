from application.data_preparation.services.config.environment.env_variables_service_implemented import \
    EnvironmentVariablesService
from application.data_preparation.services.config.lambda_function.data_preparation_enum import \
    DataPreparationConfigParams
from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.services.config.lambda_function.lambda_config_service import LambdaConfigService
from domain.data_preparation.services.config.lambda_function.lambda_config_validator_service import \
    LambdaConfigValidatorService
from domain.data_preparation.services.date_parsing_service import DataParsingService

env_service = EnvironmentVariablesService()


class LambdaConfigServiceImplemented(LambdaConfigService):
    DEFAULT_WORKERS_COUNT = 4
    DEFAULT_THREADS_PER_WORKER = 1

    def __init__(self, data_parsing_service: DataParsingService, lambda_config_validator: LambdaConfigValidatorService):
        self.data_parsing_service = data_parsing_service
        self.lambda_config_validator = lambda_config_validator

    def load_input_manifest(self) -> InputManifest:
        manifest = self.create_manifest_structure()
        self.lambda_config_validator.validate_input(manifest)
        return manifest

    def create_manifest_structure(self) -> InputManifest:
        bucket_name = env_service.load_env_variable("RAW_BUCKET_NAME")
        table_name = env_service.load_env_variable("CONTROL_TABLE_NAME")
        if not table_name:
            raise ValueError("CONTROL_TABLE_NAME environment variable is required")
        run_id = self.data_parsing_service.utc_now_compact()
        return InputManifest(
            run_id=run_id,
            source_csv_path=f"s3://{bucket_name}/{DataPreparationConfigParams.DISCOVERY_DEFAULT_KEY}",
            workers=int(self.DEFAULT_WORKERS_COUNT),
            threads_per_worker=int(self.DEFAULT_THREADS_PER_WORKER),
            bucket_name=bucket_name if bucket_name else "",
            control_table_name=table_name,
            prepared_prefix=f"{DataPreparationConfigParams.PREPARED_PREFIX}/run_id={run_id}",
            global_rate_limit=int(DataPreparationConfigParams.GLOBAL_RATE_LIMIT),
            window_seconds=int(DataPreparationConfigParams.WINDOW_SECONDS),
            calls_per_model=int(DataPreparationConfigParams.CALLS_PER_MODEL),
        )
