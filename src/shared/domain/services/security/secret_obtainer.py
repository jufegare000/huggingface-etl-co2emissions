from typing import Protocol, Optional


class SecretObtainer(Protocol):
    def get_secret_token(self, secret_name: Optional[str]) -> str:
        ...