from typing import Any, Dict, List

from domain.extract.services.metadata.models.models_metadata_sercivice import ModelsMetadataService


class ModelsMetadataServiceImplemented(ModelsMetadataService):

    __init__(self, ):


    def load_models_metadata(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = read_csv_from_s3(config["source_csv_path"])

        models_by_id: Dict[str, Dict[str, Any]] = {}

        for row in rows:
            model_id = str(row.get("model_id") or "").strip()
            emissions = safe_float(row.get("co2_eq_emissions"))

            if not model_id:
                continue

            if emissions is None:
                continue

            row["model_id"] = model_id
            row["co2_eq_emissions"] = emissions
            row["downloads"] = safe_int(row.get("downloads"))
            row["likes"] = safe_int(row.get("likes"))

            models_by_id[model_id] = row

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
            raise ValueError("No valid models found in source CSV")

        return models