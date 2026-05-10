from typing import Any, Optional


def _safe_get(d: Any, key: str, default=None):
    if isinstance(d, dict):
        return d.get(key, default)
    return default


def _normalize_datasets(card_data: dict) -> Optional[str]:
    datasets = _safe_get(card_data, "datasets")

    if datasets is None:
        return None
    if isinstance(datasets, str):
        return datasets
    if isinstance(datasets, list):
        clean = [str(x).strip() for x in datasets if x]
        return ", ".join(clean) if clean else None

    return None


def extract_raw_model_row(model) -> Optional[dict]:
    card_data = getattr(model, "cardData", None) or {}
    emissions = _safe_get(card_data, "co2_eq_emissions")

    if not emissions:
        return None

    if isinstance(emissions, dict):
        emissions_value = emissions.get("emissions")
        source = emissions.get("source")
        training_type = emissions.get("training_type")
        geographical_location = emissions.get("geographical_location")
        hardware_used = emissions.get("hardware_used")
    else:
        emissions_value = emissions
        source = None
        training_type = None
        geographical_location = None
        hardware_used = None

    return {
        "model_id": getattr(model, "id", None),
        "author": getattr(model, "author", None),
        "created_at": getattr(model, "createdAt", None),
        "last_modified": getattr(model, "lastModified", None),
        "downloads": getattr(model, "downloads", 0),
        "likes": getattr(model, "likes", 0),
        "pipeline_tag": getattr(model, "pipeline_tag", None),
        "library_name": getattr(model, "library_name", None),
        "language": str(card_data.get("language")) if card_data.get("language") else None,
        "license": str(card_data.get("license")) if card_data.get("license") else None,
        "datasets": _normalize_datasets(card_data),
        "co2_emissions_grams": emissions_value,
        "source": source,
        "training_type": training_type,
        "geographical_location": geographical_location,
        "hardware_used": hardware_used,
    }