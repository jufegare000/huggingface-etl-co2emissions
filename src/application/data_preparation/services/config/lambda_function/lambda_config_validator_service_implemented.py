from domain.data_preparation.models.preparation.input_manifest import InputManifest
from domain.data_preparation.services.config.lambda_function.lambda_config_validator_service import LambdaConfigValidatorService


class LambdaConfigValidatorServiceImplemented(LambdaConfigValidatorService):

    def __init__(self, s3_uri_service):
        self.s3_uri_service = s3_uri_service

    def validate_input(self, config: InputManifest) -> None:
        if config["workers"] <= 0:
            raise ValueError("workers must be > 0")

        if config["threads_per_worker"] <= 0:
            raise ValueError("threads_per_worker must be > 0")

        if config["global_rate_limit"] <= 0:
            raise ValueError("global_rate_limit must be > 0")

        if config["window_seconds"] <= 0:
            raise ValueError("window_seconds must be > 0")

        if config["calls_per_model"] <= 0:
            raise ValueError("calls_per_model must be > 0")

        if not config["source_csv_path"]:
            raise ValueError("source_csv_path is required")

        self.s3_uri_service.parse_s3_uri(config["source_csv_path"])
