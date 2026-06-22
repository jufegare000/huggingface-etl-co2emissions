from typing import Any, Protocol


class PartitionDescriptorService(Protocol):
    def build_partition_descriptors(
            self,
            boundaries: list[dict[str, Any]],
            config: dict[str, Any],
            models: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        ...
