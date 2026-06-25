from enum import Enum


class CheckpointStatusEnum(str, Enum):
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    RATE_LIMITED = "RATE_LIMITED"
    INTERRUPTED = "INTERRUPTED"
    COMPLETED = "COMPLETED"

    @property
    def is_resumable(self) -> bool:
        return self in {
            CheckpointStatusEnum.RUNNING,
            CheckpointStatusEnum.FAILED,
            CheckpointStatusEnum.RATE_LIMITED,
            CheckpointStatusEnum.INTERRUPTED,
        }