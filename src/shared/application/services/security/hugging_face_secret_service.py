from shared.application.services.config.env_variables_service_implemented import EnvironmentVariablesService
from shared.domain.services.security.secret_obtainer import SecretObtainer

env_service = EnvironmentVariablesService()
import logging
logger = logging.getLogger(__name__)

class HuggingFaceSecretService(SecretObtainer):

    def __init__(self, secret_obtainer_service: SecretObtainer):
        self.secret_obtainer_service = secret_obtainer_service

    def get_secret_token(self, secret_name="CUSTOMER_HF_TOKEN_SECRET_NAME"):
        secret_name_loaded = env_service.load_env_variable(secret_name)
        hf_token = self.secret_obtainer_service.get_secret_token(secret_name_loaded)
        logger.info("my token: "+hf_token)
        parsed_token = hf_token.strip().strip('"').strip("'")
        logger.info("my parsed token: " + parsed_token)
        return parsed_token

    def get_hf_token_secret_name(self) -> str | None:
        return env_service.load_env_variable("CUSTOMER_HF_TOKEN_SECRET_NAME")
