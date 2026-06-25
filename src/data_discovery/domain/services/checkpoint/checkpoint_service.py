from typing import Protocol

from data_discovery.domain.models.checkpoint_entity import CheckpointEntity


class CheckpointService(Protocol):
    def load_or_create_checkpoint(self) -> CheckpointEntity:
        ...
