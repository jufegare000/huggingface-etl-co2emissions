from typing import Optional, Dict


class RateLimitError(Exception):
    def __init__(
            self,
            status_code: int,
            retry_after: Optional[str],
            response_text: str,
            headers: Dict[str, str],
    ):
        self.status_code = status_code
        self.retry_after = retry_after
        self.response_text = response_text
        self.headers = headers
        super().__init__(
            f"Rate limited with status {status_code}. Retry-After={retry_after}"
        )