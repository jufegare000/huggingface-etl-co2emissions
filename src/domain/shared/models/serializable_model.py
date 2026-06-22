from dataclasses import asdict, is_dataclass
from enum import Enum
from datetime import datetime
from decimal import Decimal
from typing import Any


class SerializableModel:
    """
    A base class to give a way to serialize from domain model to dict
    """

    def to_dict(self) -> dict[str, Any]:
        return self._serialize(self)

    @classmethod
    def _serialize(cls, obj: Any) -> Any:
        if is_dataclass(obj):
            return cls._serialize(asdict(obj))

        if isinstance(obj, dict):
            return {k: cls._serialize(v) for k, v in obj.items()}

        if isinstance(obj, list):
            return [cls._serialize(v) for v in obj]

        if isinstance(obj, Enum):
            return obj.value

        if isinstance(obj, datetime):
            return obj.isoformat()

        if isinstance(obj, Decimal):
            return float(obj)

        return obj