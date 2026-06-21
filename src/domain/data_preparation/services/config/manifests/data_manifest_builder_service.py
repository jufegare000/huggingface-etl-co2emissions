from typing import Any, Dict, List, Protocol


class DataManifestBuilderService(Protocol):
    def build_manifest(self, partitions: List[Dict[str, Any]],
                       bucket: str,
                       config: Dict[str, Any],
                       ) -> (dict[str, str | int], str):
        ...
