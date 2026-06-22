from domain.data_preparation.models.data.input_manifest import InputManifest
from domain.data_preparation.models.data.model_metadata import ModelMetadata
from domain.data_preparation.services.metadata.ai_metadata_models.models_metadata_parser_service import \
    AIModelsMetadataParserService
from domain.data_preparation.services.metadata.ai_metadata_models.models_metadata_service import AIModelsMetadataService
from domain.data_preparation.services.plain_texts.plain_text_reader_service import PlainTextReaderService


class AIAIModelsMetadataServiceImplemented(AIModelsMetadataService):

    def __init__(self, plain_text_reader: PlainTextReaderService,
                 ai_models_metadata_parser_service: AIModelsMetadataParserService):
        self.plain_text_reader = plain_text_reader
        self.ai_models_metadat_parser_service = ai_models_metadata_parser_service

    def load_models_metadata(self, config: InputManifest) -> list[ModelMetadata]:
        rows = self.plain_text_reader.read_csv_from_s3(config["source_csv_path"])

        models_by_id: dict[str, ModelMetadata] = {}

        for row in rows:
            model_id = str(row.get("model_id") or "").strip()
            emissions = \
                (row.get("co2_eq_emissions"))

            if not model_id:
                continue

            if emissions is None:
                continue

            row["model_id"] = model_id
            row["co2_eq_emissions"] = emissions
            row["downloads"] = self.ai_models_metadat_parser_service.safe_int(row.get("downloads"))
            row["likes"] = self.ai_models_metadat_parser_service.safe_int(row.get("likes"))

            models_by_id[model_id] = ModelMetadata(row)

        models = list(models_by_id.values())

        models.sort(
            key=lambda item: (
                item.get("downloads", 0),
                item.get("likes", 0),
                item.get("co2_eq_emissions", 0),
            ),
            reverse=True,
        )

        if not models:
            raise ValueError("No valid ai_metadata_models found in source CSV")

        return models
