import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'run_id', 'control_table_name',
    'source_bucket', 'target_bucket'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

RUN_ID = args['run_id']
TABLE_NAME = args['control_table_name']
SOURCE_BUCKET = args['source_bucket']
TARGET_BUCKET = args['target_bucket']

# 1. Validación de integridad mediante DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)

response = table.query(
    KeyConditionExpression="PK = :pk AND begins_with(SK, :sk)",
    ExpressionAttributeValues={
        ":pk": f"RUN#{RUN_ID}",
        ":sk": "PARTITION#"
    }
)
partitions = response.get('Items', [])

incomplete = [p['SK'] for p in partitions if p.get('status') != 'COMPLETED']
if incomplete:
    raise Exception(f"Error de integridad: Particiones no completadas: {incomplete}")

print(f"Validación exitosa. Procesando {len(partitions)} particiones.")

input_pattern = f"s3://{SOURCE_BUCKET}/enriched/hf-carbon/run_id={RUN_ID}/partition_id=*/batches/*.jsonl"

df = spark.read.json(input_pattern)

window_spec = Window.partitionBy("modelId").orderBy(F.col("enriched_at").desc())

gold_df = df.withColumn("rank", F.row_number().over(window_spec)) \
            .filter(F.col("rank") == 1) \
            .drop("rank")

target_path = f"s3://{TARGET_BUCKET}/gold/hf-carbon/run_id={RUN_ID}/models"
gold_df.write.mode("overwrite").parquet(target_path)

quality_metrics = {
    "total_records": gold_df.count(),
    "co2_reported_count": gold_df.filter(F.col("co2_reported") == True).count(),
    "avg_performance_score": gold_df.select(F.avg("performance_score")).collect()[0][0]
}

table.update_item(
    Key={'PK': f"PIPELINE#hf-carbon", 'SK': 'LAST_RUN'},
    UpdateExpression="SET gold_status = :s, gold_completed_at = :ts, gold_path = :p",
    ExpressionAttributeValues={
        ':s': 'COMPLETED',
        ':ts': F.current_timestamp().cast("string"), # O usar datetime.now()
        ':p': target_path
    }
)

job.commit()