from typing import Any, Dict, List, Protocol


class PartitionDescriptorService(Protocol):
    def build_partition_descriptors(
            self,
            boundaries: List[Dict[str, Any]],
            config: Dict[str, Any],
            models: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        ...