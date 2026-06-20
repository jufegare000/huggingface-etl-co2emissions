from typing import Protocol

class DataParsingService(Protocol):
    def utc_now_compact(self) -> str:
        ...