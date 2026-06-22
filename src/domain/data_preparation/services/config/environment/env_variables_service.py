from typing import Protocol


class EnvVariablesService(Protocol):

    def load_env_variable(self, variable_name: str) -> str:
        ...
