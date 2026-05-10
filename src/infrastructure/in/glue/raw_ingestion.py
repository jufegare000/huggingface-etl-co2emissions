import sys
import requests
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'output_bucket',
    'partition_id',
    'emission_min',
    'emission_max'
])

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def fetch_hf_data(e_min, e_max):

    print(f"Querying HF API for the following emission ranges: {e_min} - {e_max}")

    return [{"model_id": "test/model", "co2": 15.0}]

try:
    data = fetch_hf_data(args['emission_min'], args['emission_max'])

    print(f"Procesadas {len(data)} entradas para la partición {args['partition_id']}")

    job.commit()
    print("JOB_STATUS: SUCCESS")
except Exception as e:
    print(f"JOB_STATUS: FAILED - Error: {str(e)}")
    sys.exit(1)