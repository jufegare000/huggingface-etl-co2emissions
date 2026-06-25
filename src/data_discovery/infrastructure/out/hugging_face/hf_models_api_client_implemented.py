import re
from typing import Optional, List, Dict, Any, Tuple
from urllib.parse import parse_qs, urlparse

import requests

from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from shared.domain.exceptions.rate_limit_error import RateLimitError


class HfModelsApiClientImplemented:
    def __init__(self) -> None:
        self._session = requests.Session()

    def fetch_models_page(
        self,
        hf_token: str,
        cursor: Optional[str],
    ) -> Tuple[List[Dict[str, Any]], Optional[str], Dict[str, str]]:
        headers = {
            "Authorization": f"Bearer {hf_token}",
            "Accept": "application/json",
        }
        params = {
            "full": "true",
            "cardData": "true",
            "sort": "trendingScore",
            "limit": str(DiscoveryJobConstantsEnum.PAGE_LIMIT),
        }
        if cursor:
            params["cursor"] = cursor

        response = self._session.get(
            DiscoveryJobConstantsEnum.HF_MODELS_URL,
            headers=headers,
            params=params,
            timeout=DiscoveryJobConstantsEnum.REQUEST_TIMEOUT_SECONDS,
        )
        response_headers = dict(response.headers)

        if response.status_code == 429:
            raise RateLimitError(
                status_code=429,
                retry_after=response.headers.get("Retry-After"),
                response_text=response.text,
                headers=response_headers,
            )

        if response.status_code >= 400:
            raise RuntimeError(
                f"Hugging Face API error {response.status_code}: {response.text[:1000]}"
            )

        payload = response.json()
        if not isinstance(payload, list):
            raise RuntimeError(
                f"Unexpected Hugging Face response. Expected list, got: {type(payload).__name__}"
            )

        next_cursor = _extract_next_cursor(response.headers.get("Link"))
        return payload, next_cursor, response_headers

    def retry_after_to_seconds(self, value: Optional[str]) -> int:
        if not value:
            return DiscoveryJobConstantsEnum.DEFAULT_429_SLEEP_SECONDS
        try:
            return max(int(float(value)), 1)
        except ValueError:
            return DiscoveryJobConstantsEnum.DEFAULT_429_SLEEP_SECONDS


def _extract_next_cursor(link_header: Optional[str]) -> Optional[str]:
    if not link_header:
        return None
    for part in link_header.split(","):
        part = part.strip()
        if 'rel="next"' not in part:
            continue
        match = re.search(r"<([^>]+)>", part)
        if not match:
            continue
        parsed = urlparse(match.group(1))
        cursor_values = parse_qs(parsed.query).get("cursor")
        if cursor_values:
            return cursor_values[0]
    return None
