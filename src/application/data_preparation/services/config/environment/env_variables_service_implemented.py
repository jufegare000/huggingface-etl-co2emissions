from domain.data_preparation.services.config.environment.env_variables_service import EnvVariablesService
import os


class EnvironmentVariablesService(EnvVariablesService):
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            print('Creating the object')
            cls._instance = super(EnvironmentVariablesService, cls).__new__(cls)
        return cls._instance

    def load_env_variable(self, variable_name) -> str:
        return os.environ.get(variable_name)
