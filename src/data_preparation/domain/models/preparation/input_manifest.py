from dataclasses import dataclass

@dataclass
class InputManifest:
    run_id: str
    source_csv_path: str
    workers: int
    threads_per_worker: int
    bucket_name: str
    control_table_name: str
    prepared_prefix: str
    global_rate_limit: int
    window_seconds: int
    calls_per_model: int