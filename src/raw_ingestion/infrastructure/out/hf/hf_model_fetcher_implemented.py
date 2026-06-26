import json
from typing import Any, Dict
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
import logging

from raw_ingestion.domain.services.hf_model_fetcher import HfModelFetcher

HF_MODEL_API_BASE_URL = "https://huggingface.co/api/models"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 60
logger = logging.getLogger(__name__)

class HfModelFetcherImplemented(HfModelFetcher):

    def fetch_model(self, model_id: str, hf_token: str) -> Dict[str, Any]:
        encoded_model_id = quote(model_id, safe="/")
        url = f"{HF_MODEL_API_BASE_URL}/{encoded_model_id}?full=true&cardData=true"

        request = Request(
            url=url,
            headers={
                "Authorization": f"Bearer {hf_token}",
                "Accept": "application/json",
            },
            method="GET",
        )

        try:
            with urlopen(request, timeout=DEFAULT_REQUEST_TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))

        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"HF API HTTP {exc.code} for model_id={model_id}: {body[:500]}"
            ) from exc

        except URLError as exc:
            raise RuntimeError(f"HF API URL error for model_id={model_id}: {exc}") from exc
