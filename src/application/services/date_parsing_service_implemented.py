from datetime import datetime, timezone

from domain.extract.services.date_parsing_service import DataParsingService


class SystemDateTimeServiceImplemented(DataParsingService):
    def utc_now_compact(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")