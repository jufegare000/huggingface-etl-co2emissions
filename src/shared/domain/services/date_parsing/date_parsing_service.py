from datetime import datetime
from typing import Protocol

class DataParsingService(Protocol):
    def utc_now_compact(self) -> str:
        ...

    def utc_now_iso(self) -> str:
        ...

    def utc_now(self) -> datetime:
        ...
