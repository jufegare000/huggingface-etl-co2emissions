import json
from typing import Any, Dict, List, Optional


class RowEnrichmentService:

    def build_enriched_row(
        self,
        row: Dict[str, Any],
        hf_payload: Dict[str, Any],
        run_id: str,
        partition_id: str,
        enriched_at: str,
    ) -> Dict[str, Any]:
        card_data = self._extract_card_data(hf_payload)
        datasets = self._extract_datasets(card_data, hf_payload)
        datasets_size = len(datasets)
        performance_metrics = self._extract_performance_metrics(card_data)
        performance_score = self._extract_performance_score(performance_metrics)
        size = self._extract_model_size(hf_payload)
        downloads = self._safe_int(hf_payload.get("downloads") or row.get("downloads"))
        likes = self._safe_int(hf_payload.get("likes") or row.get("likes"))
        co2_eq_emissions = self._safe_float(row.get("co2_eq_emissions"))

        size_efficency = (downloads / size) if size and size > 0 else None
        datasets_size_efficency = (downloads / datasets_size) if datasets_size > 0 else None

        return {
            "model_id": hf_payload.get("id") or row.get("model_id"),
            "datasets": self._safe_json(datasets),
            "datasets_size": datasets_size,
            "co2_eq_emissions": co2_eq_emissions,
            "co2_reported": co2_eq_emissions is not None,
            "co2_source": row.get("co2_source"),
            "training_type": row.get("training_type"),
            "geographical_location": row.get("geographical_location"),
            "environment": card_data.get("environment"),
            "performance_metrics": self._safe_json(performance_metrics),
            "performance_score": performance_score,
            "downloads": downloads,
            "likes": likes,
            "library_name": hf_payload.get("library_name") or row.get("library_name"),
            "domain": self._infer_domain(row, hf_payload),
            "size": size,
            "created_at": (
                hf_payload.get("createdAt") or hf_payload.get("created_at") or row.get("created_at")
            ),
            "size_efficency": size_efficency,
            "datasets_size_efficency": datasets_size_efficency,
            "auto": self._infer_auto(row, hf_payload),
            "run_id": run_id,
            "partition_id": partition_id,
            "enriched_at": enriched_at,
            "hardware_used": row.get("hardware_used"),
            "pipeline_tag": row.get("pipeline_tag"),
            "tags": row.get("tags"),
            "snapshot_id": row.get("snapshot_id"),
            "discovered_at": row.get("discovered_at"),
        }

    def build_error_row(
        self,
        row: Dict[str, Any],
        error: Exception,
        run_id: str,
        partition_id: str,
        failed_at: str,
    ) -> Dict[str, Any]:
        return {
            "run_id": run_id,
            "partition_id": partition_id,
            "model_id": row.get("model_id"),
            "error_type": type(error).__name__,
            "error_message": str(error),
            "failed_at": failed_at,
        }

    def _extract_card_data(self, hf_payload: Dict[str, Any]) -> Dict[str, Any]:
        card_data = hf_payload.get("cardData") or hf_payload.get("card_data") or {}
        return card_data if isinstance(card_data, dict) else {}

    def _extract_datasets(
        self, card_data: Dict[str, Any], hf_payload: Dict[str, Any]
    ) -> List[Any]:
        datasets = (
            card_data.get("datasets")
            or card_data.get("dataset")
            or hf_payload.get("datasets")
            or []
        )
        if isinstance(datasets, list):
            return datasets
        if isinstance(datasets, str):
            return [datasets]
        return []

    def _extract_performance_metrics(self, card_data: Dict[str, Any]) -> Any:
        for key in ("model-index", "model_index", "eval_results", "metrics", "performance"):
            value = card_data.get(key)
            if value:
                return value
        return None

    def _extract_performance_score(self, metrics: Any) -> Optional[float]:
        if metrics is None:
            return None
        numbers: List[float] = []

        def collect(value: Any) -> None:
            if isinstance(value, (int, float)):
                numbers.append(float(value))
            elif isinstance(value, dict):
                for nested in value.values():
                    collect(nested)
            elif isinstance(value, list):
                for nested in value:
                    collect(nested)

        collect(metrics)
        return max(numbers) if numbers else None

    def _extract_model_size(self, hf_payload: Dict[str, Any]) -> Optional[int]:
        safetensors = hf_payload.get("safetensors")
        if isinstance(safetensors, dict):
            total = safetensors.get("total")
            if isinstance(total, int):
                return total

        siblings = hf_payload.get("siblings") or []
        total_size = 0
        found = False
        if isinstance(siblings, list):
            for item in siblings:
                if isinstance(item, dict) and isinstance(item.get("size"), int):
                    total_size += item["size"]
                    found = True
        return total_size if found else None

    def _infer_domain(self, row: Dict[str, Any], hf_payload: Dict[str, Any]) -> Optional[str]:
        pipeline_tag = row.get("pipeline_tag") or hf_payload.get("pipeline_tag")
        if pipeline_tag:
            return str(pipeline_tag)
        tags = hf_payload.get("tags") or row.get("tags")
        if isinstance(tags, list) and tags:
            return str(tags[0])
        if isinstance(tags, str) and tags:
            return tags[:100]
        return None

    def _infer_auto(self, row: Dict[str, Any], hf_payload: Dict[str, Any]) -> bool:
        tags = hf_payload.get("tags") or row.get("tags") or []
        if isinstance(tags, str):
            tags_text = tags.lower()
        else:
            tags_text = " ".join(str(tag).lower() for tag in tags)
        return "autotrain" in tags_text or "auto" in tags_text

    def _safe_json(self, value: Any) -> str:
        if value is None:
            return ""
        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return json.dumps(str(value), ensure_ascii=False)

    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(str(value).strip())
        except ValueError:
            return None

    def _safe_int(self, value: Any) -> int:
        if value is None:
            return 0
        try:
            return int(float(str(value).strip()))
        except ValueError:
            return 0
