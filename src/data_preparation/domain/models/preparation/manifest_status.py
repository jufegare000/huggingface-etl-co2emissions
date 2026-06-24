from enum import StrEnum

class ManifestStatus(StrEnum):
    PREPARED = "PREPARED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"