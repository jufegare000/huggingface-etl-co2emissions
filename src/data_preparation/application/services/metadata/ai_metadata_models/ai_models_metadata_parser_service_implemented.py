from data_preparation.domain.services.metadata.ai_metadata_models.models_metadata_parser_service import \
    AIModelsMetadataParserService
from typing import Any


class AIModelsMetadataParserServiceImplemented(AIModelsMetadataParserService):
    def safe_float(self, value: Any) -> float | None:
        if value is None:
            return None

        try:
            return float(str(value).strip())
        except ValueError:
            return None

    def safe_int(self, value: Any) -> int:
        if value is None:
            return 0

        try:
            return int(float(str(value).strip()))
        except ValueError:
            return 0
