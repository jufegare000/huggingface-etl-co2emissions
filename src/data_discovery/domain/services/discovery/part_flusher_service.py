from typing import Protocol, Dict, Any

from data_discovery.domain.models.discovery_run_state import DiscoveryRunState


class PartFlusherService(Protocol):
    def flush_if_pending(self, state: DiscoveryRunState, checkpoint: Dict[str, Any]) -> None: ...
    def flush_on_condition(self, state: DiscoveryRunState, checkpoint: Dict[str, Any]) -> None: ...
