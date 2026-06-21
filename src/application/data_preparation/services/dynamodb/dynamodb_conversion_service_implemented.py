from decimal import Decimal
from infrastructure.out.dynamo.parsing_utils.dynamo_value import DynamoDBInputValue, DynamoDBSafeValue
from infrastructure.out.dynamo.services.type_conversion_service import TypeConversionService

class DynamoDBTypeConversionServiceImplemented(TypeConversionService):

    def convert_floats_to_decimal(
            self,
            value: DynamoDBInputValue,
    ) -> DynamoDBSafeValue:
        if isinstance(value, float):
            return Decimal(str(value))

        if isinstance(value, dict):
            return {
                key: self.convert_floats_to_decimal(nested_value)
                for key, nested_value in value.items()
            }

        if isinstance(value, list):
            return [
                self.convert_floats_to_decimal(nested_value)
                for nested_value in value
            ]

        return value
