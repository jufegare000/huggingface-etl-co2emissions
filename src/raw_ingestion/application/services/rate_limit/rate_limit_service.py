import time
from typing import Tuple


class RateLimitService:

    def sleep_if_budget_reached(
        self,
        api_calls_in_window: int,
        partition_budget: int,
        window_started_at: float,
        window_seconds: int,
    ) -> Tuple[int, float]:
        if api_calls_in_window < partition_budget:
            return api_calls_in_window, window_started_at

        elapsed = time.time() - window_started_at
        remaining = window_seconds - elapsed

        if remaining > 0:
            print(f"Rate window budget reached. Sleeping {remaining:.2f} seconds.")
            time.sleep(remaining)

        return 0, time.time()
