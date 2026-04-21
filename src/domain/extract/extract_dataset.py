from typing import Any, Optional

def safe_get(d: Any, key: str, default=None):
    if isinstance(d, dict):
        return d.get(key, default)
    return default


def normalize_to_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def extract_datasets(info) -> Optional[str]:
    card_data = getattr(info, "cardData", None) or {}
    datasets = safe_get(card_data, "datasets")

    if datasets:
        datasets = normalize_to_list(datasets)
        datasets = [str(x) for x in datasets if x is not None]
        datasets = sorted(set(datasets))
        return ", ".join(datasets) if datasets else None

    tags = getattr(info, "tags", None) or []
    datasets_from_tags = []
    for tag in tags:
        if isinstance(tag, str) and tag.startswith("dataset:"):
            datasets_from_tags.append(tag.replace("dataset:", ""))

    datasets_from_tags = sorted(set(datasets_from_tags))
    return ", ".join(datasets_from_tags) if datasets_from_tags else None