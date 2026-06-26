from typing import Any, Dict, Protocol


class HfModelFetcher(Protocol):
    def fetch_model(self, model_id: str, hf_token: str) -> Dict[str, Any]:
        ...
