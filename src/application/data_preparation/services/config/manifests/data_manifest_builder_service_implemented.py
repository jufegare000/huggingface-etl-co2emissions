from typing import Any, Dict, List, Protocol


class DataManifestBuilderServiceImplemented(Protocol):
    def build_manifest(self, partitions: List[Dict[str, Any]],
                       bucket: str,
                       config: Dict[str, Any],
                       ) -> (dict[str, str | int], str):
        ...
