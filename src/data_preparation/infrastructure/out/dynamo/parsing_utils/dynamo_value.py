from decimal import Decimal
from typing import TypeAlias


DynamoDBInputValue: TypeAlias = (
    str
    | int
    | float
    | bool
    | None
    | Decimal
    | list["DynamoDBInputValue"]
    | dict[str, "DynamoDBInputValue"]
)

DynamoDBSafeValue: TypeAlias = (
    str
    | int
    | bool
    | None
    | Decimal
    | list["DynamoDBSafeValue"]
    | dict[str, "DynamoDBSafeValue"]
)
