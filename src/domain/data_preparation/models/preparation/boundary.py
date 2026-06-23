from dataclasses import dataclass

@dataclass
class Boundary:
    partition_id: int
    start_index: int
    end_index: int
    records_count: int
    emission_min: float | None
    emission_max: float | None