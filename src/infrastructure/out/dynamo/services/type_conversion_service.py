from typing import Protocol

from infrastructure.out.dynamo.parsing_utils.dynamo_value import (
    DynamoDBInputValue,
    DynamoDBSafeValue,
)

class TypeConversionService(Protocol):
    def convert_floats_to_decimal(
        self,
        value: DynamoDBInputValue,
    ) -> DynamoDBSafeValue:
        ...