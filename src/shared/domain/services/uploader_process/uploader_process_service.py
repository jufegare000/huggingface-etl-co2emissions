from typing import Protocol, List, Dict, Any, Optional


class UploaderProcessService(Protocol):
    def upload_rows_part(
            self,
            snapshot_id: str,
            part_number: int,
            rows: List[Dict[str, Any]],
    ) -> Optional[str]:
        ...

    def rows_to_csv_text(self, rows: List[Dict[str, Any]]) -> str:
        ...