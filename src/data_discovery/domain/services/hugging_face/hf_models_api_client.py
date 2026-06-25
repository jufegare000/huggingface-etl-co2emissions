from typing import Protocol, Optional, List, Dict, Any, Tuple


class HfModelsApiClient(Protocol):
    def fetch_models_page(
        self,
        hf_token: str,
        cursor: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], Optional[str], Dict[str, str]]:
        ...

    def retry_after_to_seconds(self, value: Optional[str]) -> int:
        ...
