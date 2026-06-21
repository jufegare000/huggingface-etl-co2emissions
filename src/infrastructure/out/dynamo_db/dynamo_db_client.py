import boto3


class DynamoDBClient:
    _instance = None
    dynamodb_client = boto3.resource("dynamo_db")

    def __new__(cls):
        if cls._instance is None:
            print('Creating the object')
            cls._instance = super(DynamoDBClient, cls).__new__(cls)
        return cls._instance

    def get_dynamo_db_client(self) -> str:
        return self.dynamodb_client
