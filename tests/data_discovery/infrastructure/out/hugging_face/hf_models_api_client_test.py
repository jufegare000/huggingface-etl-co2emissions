from unittest.mock import MagicMock, patch

import pytest

from data_discovery.application.services.discovery.discovery_job_constants_enum import DiscoveryJobConstantsEnum
from data_discovery.infrastructure.out.hugging_face.hf_models_api_client_implemented import (
    HfModelsApiClientImplemented,
    _extract_next_cursor,
)
from shared.domain.exceptions.rate_limit_error import RateLimitError

HF_TOKEN = "hf_fake_token"
CURSOR = "cursor_abc123"


def _make_response(status_code=200, json_body=None, headers=None, text=""):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_body if json_body is not None else []
    resp.headers = headers or {}
    resp.text = text
    return resp


@pytest.fixture
def client():
    c = HfModelsApiClientImplemented()
    c._session = MagicMock()
    return c


# --- fetch_models_page: success ---

def test_fetch_models_page_returns_models_and_cursor(client):
    models = [{"modelId": "org/model-a"}]
    link = f'<https://huggingface.co/api/models?cursor={CURSOR}>; rel="next"'
    client._session.get.return_value = _make_response(json_body=models, headers={"Link": link})

    result_models, next_cursor, _ = client.fetch_models_page(HF_TOKEN, cursor=None)

    assert result_models == models
    assert next_cursor == CURSOR


def test_fetch_models_page_passes_cursor_param(client):
    client._session.get.return_value = _make_response()
    client.fetch_models_page(HF_TOKEN, cursor=CURSOR)

    params = client._session.get.call_args.kwargs["params"]
    assert params["cursor"] == CURSOR


def test_fetch_models_page_omits_cursor_when_none(client):
    client._session.get.return_value = _make_response()
    client.fetch_models_page(HF_TOKEN, cursor=None)

    params = client._session.get.call_args.kwargs["params"]
    assert "cursor" not in params


def test_fetch_models_page_includes_auth_header(client):
    client._session.get.return_value = _make_response()
    client.fetch_models_page(HF_TOKEN, cursor=None)

    headers = client._session.get.call_args.kwargs["headers"]
    assert headers["Authorization"] == f"Bearer {HF_TOKEN}"


def test_fetch_models_page_returns_empty_cursor_when_no_link(client):
    client._session.get.return_value = _make_response(headers={})
    _, next_cursor, _ = client.fetch_models_page(HF_TOKEN, cursor=None)
    assert next_cursor is None


# --- fetch_models_page: error paths ---

def test_fetch_models_page_raises_rate_limit_error_on_429(client):
    client._session.get.return_value = _make_response(
        status_code=429,
        headers={"Retry-After": "60"},
        text="Too Many Requests",
    )
    with pytest.raises(RateLimitError) as exc_info:
        client.fetch_models_page(HF_TOKEN, cursor=None)

    assert exc_info.value.status_code == 429
    assert exc_info.value.retry_after == "60"


def test_fetch_models_page_raises_runtime_error_on_4xx(client):
    client._session.get.return_value = _make_response(status_code=403, text="Forbidden")
    with pytest.raises(RuntimeError, match="403"):
        client.fetch_models_page(HF_TOKEN, cursor=None)


def test_fetch_models_page_raises_when_response_is_not_list(client):
    client._session.get.return_value = _make_response(json_body={"error": "unexpected"})
    with pytest.raises(RuntimeError, match="Expected list"):
        client.fetch_models_page(HF_TOKEN, cursor=None)


# --- retry_after_to_seconds ---

def test_retry_after_to_seconds_parses_integer_string():
    client = HfModelsApiClientImplemented()
    assert client.retry_after_to_seconds("120") == 120


def test_retry_after_to_seconds_parses_float_string():
    client = HfModelsApiClientImplemented()
    assert client.retry_after_to_seconds("90.5") == 90


def test_retry_after_to_seconds_returns_default_when_none():
    client = HfModelsApiClientImplemented()
    assert client.retry_after_to_seconds(None) == DiscoveryJobConstantsEnum.DEFAULT_429_SLEEP_SECONDS


def test_retry_after_to_seconds_returns_default_when_unparseable():
    client = HfModelsApiClientImplemented()
    assert client.retry_after_to_seconds("not-a-number") == DiscoveryJobConstantsEnum.DEFAULT_429_SLEEP_SECONDS


def test_retry_after_to_seconds_minimum_is_one():
    client = HfModelsApiClientImplemented()
    assert client.retry_after_to_seconds("0") == 1


# --- _extract_next_cursor ---

def test_extract_next_cursor_returns_cursor_from_link_header():
    link = f'<https://huggingface.co/api/models?cursor={CURSOR}&limit=1000>; rel="next"'
    assert _extract_next_cursor(link) == CURSOR


def test_extract_next_cursor_returns_none_when_no_next_rel():
    link = '<https://huggingface.co/api/models?cursor=prev>; rel="prev"'
    assert _extract_next_cursor(link) is None


def test_extract_next_cursor_returns_none_when_link_is_none():
    assert _extract_next_cursor(None) is None


def test_extract_next_cursor_returns_none_when_link_is_empty():
    assert _extract_next_cursor("") is None


def test_extract_next_cursor_handles_multiple_rels():
    link = (
        '<https://huggingface.co/api/models?cursor=prev_cur>; rel="prev", '
        f'<https://huggingface.co/api/models?cursor={CURSOR}>; rel="next"'
    )
    assert _extract_next_cursor(link) == CURSOR
