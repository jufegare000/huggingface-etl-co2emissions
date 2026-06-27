import json
from io import BytesIO
from unittest.mock import MagicMock, patch
from urllib.error import HTTPError, URLError

import pytest

from raw_ingestion.domain.models.exceptions import RateLimitError
from raw_ingestion.infrastructure.out.hf.hf_model_fetcher_implemented import (
    HfModelFetcherImplemented,
    MAX_RETRIES,
)

MODEL_ID = "org/my-model"
HF_TOKEN = "hf_fake"
HF_PAYLOAD = {"id": MODEL_ID, "downloads": 100}


def _make_http_error(code: int, body: str = "") -> HTTPError:
    return HTTPError(
        url=f"https://huggingface.co/api/models/{MODEL_ID}",
        code=code,
        msg="",
        hdrs={},
        fp=BytesIO(body.encode()),
    )


def _make_response(payload: dict) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    mock_resp.read.return_value = json.dumps(payload).encode("utf-8")
    return mock_resp


@patch("raw_ingestion.infrastructure.out.hf.hf_model_fetcher_implemented.time.sleep")
@patch("raw_ingestion.infrastructure.out.hf.hf_model_fetcher_implemented.urlopen")
class TestHfModelFetcherImplemented:

    def test_successful_fetch_returns_payload(self, mock_urlopen, mock_sleep):
        mock_urlopen.return_value = _make_response(HF_PAYLOAD)
        result = HfModelFetcherImplemented().fetch_model(MODEL_ID, HF_TOKEN)
        assert result == HF_PAYLOAD
        mock_sleep.assert_not_called()

    def test_429_retries_and_succeeds_on_second_attempt(self, mock_urlopen, mock_sleep):
        mock_urlopen.side_effect = [_make_http_error(429), _make_response(HF_PAYLOAD)]
        result = HfModelFetcherImplemented().fetch_model(MODEL_ID, HF_TOKEN)
        assert result == HF_PAYLOAD
        assert mock_sleep.call_count == 1

    def test_429_exhausts_all_retries_and_raises(self, mock_urlopen, mock_sleep):
        mock_urlopen.side_effect = _make_http_error(429)
        with pytest.raises(RateLimitError, match="HTTP 429"):
            HfModelFetcherImplemented().fetch_model(MODEL_ID, HF_TOKEN)
        assert mock_sleep.call_count == MAX_RETRIES

    def test_non_429_http_error_raises_immediately_without_retry(self, mock_urlopen, mock_sleep):
        mock_urlopen.side_effect = _make_http_error(404, "not found")
        with pytest.raises(RuntimeError, match="HTTP 404"):
            HfModelFetcherImplemented().fetch_model(MODEL_ID, HF_TOKEN)
        mock_sleep.assert_not_called()

    def test_url_error_raises_immediately_without_retry(self, mock_urlopen, mock_sleep):
        mock_urlopen.side_effect = URLError("connection refused")
        with pytest.raises(RuntimeError, match="URL error"):
            HfModelFetcherImplemented().fetch_model(MODEL_ID, HF_TOKEN)
        mock_sleep.assert_not_called()

    def test_error_message_includes_model_id(self, mock_urlopen, mock_sleep):
        mock_urlopen.side_effect = _make_http_error(500, "server error")
        with pytest.raises(RuntimeError, match=MODEL_ID):
            HfModelFetcherImplemented().fetch_model(MODEL_ID, HF_TOKEN)

    def test_429_sleep_duration_increases_with_attempts(self, mock_urlopen, mock_sleep):
        mock_urlopen.side_effect = [
            _make_http_error(429),
            _make_http_error(429),
            _make_response(HF_PAYLOAD),
        ]
        HfModelFetcherImplemented().fetch_model(MODEL_ID, HF_TOKEN)
        assert mock_sleep.call_count == 2
        first_wait = mock_sleep.call_args_list[0].args[0]
        second_wait = mock_sleep.call_args_list[1].args[0]
        assert second_wait > first_wait
