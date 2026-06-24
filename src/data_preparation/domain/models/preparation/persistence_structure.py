from dataclasses import dataclass


@dataclass
class PersistenceStructure:
    manifest_path: str
    partitions_count: int
