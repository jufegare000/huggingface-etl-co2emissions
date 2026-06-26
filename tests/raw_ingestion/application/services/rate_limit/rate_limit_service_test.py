import time
from unittest.mock import patch

import pytest

from raw_ingestion.application.services.rate_limit.rate_limit_service import RateLimitService


@pytest.fixture
def service():
    return RateLimitService()


def test_no_sleep_when_budget_not_reached(service):
    calls, started = service.sleep_if_budget_reached(
        api_calls_in_window=5,
        partition_budget=10,
        window_started_at=time.time(),
        window_seconds=300,
    )
    assert calls == 5


def test_returns_same_started_at_when_budget_not_reached(service):
    t = time.time()
    _, started = service.sleep_if_budget_reached(
        api_calls_in_window=0,
        partition_budget=10,
        window_started_at=t,
        window_seconds=300,
    )
    assert started == t


def test_resets_counter_to_zero_after_sleep(service):
    fixed_time = 1_000_000.0
    with patch("raw_ingestion.application.services.rate_limit.rate_limit_service.time.sleep"):
        with patch(
            "raw_ingestion.application.services.rate_limit.rate_limit_service.time.time",
            return_value=fixed_time,
        ):
            calls, _ = service.sleep_if_budget_reached(
                api_calls_in_window=10,
                partition_budget=10,
                window_started_at=fixed_time - 400,
                window_seconds=300,
            )
    assert calls == 0


def test_no_sleep_when_window_already_elapsed(service):
    with patch(
        "raw_ingestion.application.services.rate_limit.rate_limit_service.time.sleep"
    ) as mock_sleep:
        service.sleep_if_budget_reached(
            api_calls_in_window=10,
            partition_budget=10,
            window_started_at=time.time() - 400,
            window_seconds=300,
        )
    mock_sleep.assert_not_called()


def test_sleeps_for_remaining_window_time(service):
    window_seconds = 300
    started_at = time.time() - 100

    with patch(
        "raw_ingestion.application.services.rate_limit.rate_limit_service.time.sleep"
    ) as mock_sleep:
        with patch(
            "raw_ingestion.application.services.rate_limit.rate_limit_service.time.time",
            return_value=started_at + 100,
        ):
            service.sleep_if_budget_reached(
                api_calls_in_window=5,
                partition_budget=5,
                window_started_at=started_at,
                window_seconds=window_seconds,
            )

    mock_sleep.assert_called_once()
    slept = mock_sleep.call_args.args[0]
    assert slept == pytest.approx(200, abs=2)
