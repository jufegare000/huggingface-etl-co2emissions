import sys
from typing import Any

from black import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config.injection.dependency_injector import data_preparation_config_use_case


def handler(_event: dict[str, Any], _context: Any) -> dict[str, Any]:
    return data_preparation_config_use_case.create_configuration_for_etl()
