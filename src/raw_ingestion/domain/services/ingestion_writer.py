from typing import Any, Dict, List, Protocol


class IngestionS3Writer(Protocol):
    def write_batch(self, rows: List[Dict[str, Any]], bucket: str, key: str) -> None:
        ...

    def write_errors(self, rows: List[Dict[str, Any]], bucket: str, key: str) -> None:
        ...

    def write_metrics(self, payload: Dict[str, Any], bucket: str, key: str) -> None:
        ...

    def write_success_marker(self, payload: Dict[str, Any], bucket: str, key: str) -> None:
        ...

    def read_partition_csv(self, bucket: str, key: str) -> List[Dict[str, Any]]:
        ...

    def list_error_files(self, bucket: str, run_id: str) -> List[str]:
        ...

    def read_jsonl_lines(self, bucket: str, key: str) -> List[Dict[str, Any]]:
        ...
