from typing import Protocol, Dict, Any

from data_discovery.domain.models.discovery_run_state import DiscoveryRunState
from shared.domain.exceptions.rate_limit_error import RateLimitError


class RateLimitHandlerService(Protocol):
    def handle(
        self,
        exc: RateLimitError,
        state: DiscoveryRunState,
        checkpoint: Dict[str, Any],
    ) -> None: ...
