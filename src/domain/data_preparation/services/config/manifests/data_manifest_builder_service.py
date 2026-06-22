from typing import Any, Protocol


class DataManifestBuilderService(Protocol):
    def build_manifest(self, partitions: list[dict[str, Any]],
                       bucket: str,
                       config: dict[str, Any],
                       ) -> tuple[dict[str, str | int], str]:
        ...
