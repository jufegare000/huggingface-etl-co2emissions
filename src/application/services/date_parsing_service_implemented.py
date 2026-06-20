from datetime import datetime, timezone

class SystemDateTimeServiceImplemented():
    def utc_now_compact(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")