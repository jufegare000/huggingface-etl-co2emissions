from dataclasses import dataclass
from typing import Optional


@dataclass
class RateBudget:
    partition_call_budget: int
    window_seconds: int


@dataclass
class PartitionConfig:
    run_id: str
    partition_id: str
    input_path: str
    records_count: int
    rate_budget: Optional[RateBudget] = None
