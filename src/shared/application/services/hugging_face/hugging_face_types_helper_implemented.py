import json
from typing import Any, Optional, Dict, List, Tuple

from shared.domain.services.hugging_face.hugging_face_type_helpers import HuggingFaceTypesHelpers


class HuggingFaceTypesHelperImplemented(HuggingFaceTypesHelpers):

    def to_iso(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        return str(value)

    def safe_json(self, value: Any) -> str:
        if value is None:
            return ""

        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return json.dumps(str(value), ensure_ascii=False)

    def normalize_co2_emissions(self, value: Any) -> Optional[float]:
        if value is None:
            return None

        if isinstance(value, (int, float)):
            return float(value)

        if isinstance(value, str):
            cleaned = value.strip().lower()
            cleaned = cleaned.replace("kg", "")
            cleaned = cleaned.replace("grams", "")
            cleaned = cleaned.replace("gram", "")
            cleaned = cleaned.replace("g", "")
            cleaned = cleaned.replace("co2eq", "")
            cleaned = cleaned.replace("co2e", "")
            cleaned = cleaned.replace("co?eq", "")
            cleaned = cleaned.replace("co?e", "")
            cleaned = cleaned.replace(",", "")
            cleaned = cleaned.strip()

            try:
                return float(cleaned)
            except ValueError:
                return None

        return None

    def extract_co2_fields(self, card_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        co2 = card_data.get("co2_eq_emissions")

        if not co2 or not isinstance(co2, dict):
            return None

        emissions = self.normalize_co2_emissions(co2.get("emissions"))

        if emissions is None:
            return None

        return {
            "co2_eq_emissions": emissions,
            "co2_source": co2.get("source"),
            "training_type": co2.get("training_type"),
            "geographical_location": co2.get("geographical_location"),
            "hardware_used": co2.get("hardware_used"),
        }

    def filter_page(
        self,
        models: List[Dict[str, Any]],
        snapshot_id: str,
        discovered_at: str,
    ) -> Tuple[List[Dict[str, Any]], int]:
        matched = [
            row for model in models
            if (row := self.model_to_row(model=model, snapshot_id=snapshot_id, discovered_at=discovered_at)) is not None
        ]
        return matched, len(matched)

    def model_to_row(self, model: Dict[str, Any], snapshot_id: str, discovered_at: str) -> Optional[Dict[str, Any]]:
        card_data = model.get("cardData") or model.get("card_data") or {}

        if not isinstance(card_data, dict):
            return None

        co2_fields = self.extract_co2_fields(card_data)

        if co2_fields is None:
            return None

        return {
            "model_id": model.get("id") or model.get("modelId"),
            "co2_eq_emissions": co2_fields["co2_eq_emissions"],
            "co2_source": co2_fields["co2_source"],
            "training_type": co2_fields["training_type"],
            "geographical_location": co2_fields["geographical_location"],
            "hardware_used": co2_fields["hardware_used"],
            "created_at": self.to_iso(model.get("createdAt") or model.get("created_at")),
            "downloads": model.get("downloads"),
            "likes": model.get("likes"),
            "library_name": model.get("library_name") or model.get("libraryName"),
            "pipeline_tag": model.get("pipeline_tag") or model.get("pipelineTag"),
            "tags": self.safe_json(model.get("tags")),
            "snapshot_id": snapshot_id,
            "discovered_at": discovered_at,
        }
