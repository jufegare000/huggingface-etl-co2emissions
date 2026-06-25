import pytest

from shared.application.services.config.env_variables_service_implemented import \
    EnvironmentVariablesService

VAR_NAME = "TARGET_ENVIRONMENT_VARIABLE"
VAR_VALUE = "secure_vault_token_123"
MISSING_VAR_NAME = "NON_EXISTENT_SYSTEM_VARIABLE"


@pytest.fixture(autouse=True)
def reset_singleton_instance():
    EnvironmentVariablesService._instance = None
    yield
    EnvironmentVariablesService._instance = None


def test_singleton_returns_same_instance():
    first_instance = EnvironmentVariablesService()
    second_instance = EnvironmentVariablesService()
    assert first_instance is second_instance


def test_load_env_variable_returns_value_when_exists(monkeypatch):
    monkeypatch.setenv(VAR_NAME, VAR_VALUE)
    service = EnvironmentVariablesService()
    result = service.load_env_variable(VAR_NAME)
    assert result == VAR_VALUE


def test_load_env_variable_returns_none_when_missing():
    service = EnvironmentVariablesService()
    result = service.load_env_variable(MISSING_VAR_NAME)
    assert result is None