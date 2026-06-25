from typing import Protocol, Any, Optional, Dict, List, Tuple


class HuggingFaceTypesHelpers(Protocol):
    def to_iso(self, value: Any) -> Optional[str]:
        ...

    def safe_json(self, value: Any) -> str:
        ...

    def normalize_co2_emissions(self, value: Any) -> Optional[float]:
        ...

    def extract_co2_fields(self, card_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        ...

    def model_to_row(self, model: Dict[str, Any], snapshot_id: str, discovered_at: str) -> Optional[Dict[str, Any]]:
        ...

    def filter_page(
        self,
        models: List[Dict[str, Any]],
        snapshot_id: str,
        discovered_at: str,
    ) -> Tuple[List[Dict[str, Any]], int]:
        ...