import sys
import boto3
import json
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'hf_token_secret_name',
    'partition_id',
    'emission_min',
    'emission_max'
])


def get_hf_token(secret_name):
    client = boto3.client('secretsmanager')
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error recuperando el secreto: {e}")
        raise e

    return get_secret_value_response['SecretString']


sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

hf_token = get_hf_token(args['hf_token_secret_name'])

print(f"Token recuperado con éxito para la partición {args['partition_id']}")


job.commit()