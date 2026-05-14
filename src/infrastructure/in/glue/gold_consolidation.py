import sys
import boto3
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, StringType

# 1. Recuperar argumentos del Job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'run_id', 'control_table_name', 'source_bucket', 'target_bucket'
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

# 2. Validación de integridad
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)

response = table.query(
    KeyConditionExpression="PK = :pk AND begins_with(SK, :sk)",
    ExpressionAttributeValues={":pk": f"RUN#{RUN_ID}", ":sk": "PARTITION#"}
)
partitions = response.get('Items', [])
incomplete = [p['SK'] for p in partitions if p.get('status') != 'COMPLETED']
if incomplete:
    raise Exception(f"Integridad fallida: Particiones incompletas: {incomplete}")

# 3. Lectura de datos
input_pattern = f"s3://{SOURCE_BUCKET}/enriched/hf-carbon/run_id={RUN_ID}/partition_id=*/batches/*.jsonl"
df = spark.read.json(input_pattern)

# 4. Transformación y Limpieza
df_parsed = df.withColumn(
    "datasets_clean",
    F.from_json(F.regexp_replace(F.col("datasets"), r'\\"', '"'), ArrayType(StringType()))
)

# 5. Deduplicación
window_spec = Window.partitionBy("modelId").orderBy(F.col("enriched_at").desc())
gold_df = df_parsed.withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .withColumn("datasets_size", F.size(F.col("datasets_clean"))) \
    .withColumnRenamed("datasets_clean", "datasets_list") \
    .drop("rank", "datasets")

# 6. Escritura Técnica (PARQUET)
target_path_parquet = f"s3://{TARGET_BUCKET}/gold/hf-carbon/run_id={RUN_ID}/models"
gold_df.write.mode("overwrite").parquet(target_path_parquet)

# 7. EXPORTACIÓN A CSV (Para LibreOffice)
# Aplanamos el array a string para que el CSV sea compatible
export_df = gold_df.withColumn("datasets_list", F.col("datasets_list").cast("string")) \
                   .withColumn("performance_metrics", F.col("performance_metrics").cast("string"))

target_path_csv = f"s3://{TARGET_BUCKET}/gold/hf-carbon/run_id={RUN_ID}/export_csv"

export_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ";") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv(target_path_csv)

print(f"CSV ultra-compatible generado en: {target_path_csv}")
# 8. Registro final en DynamoDB
total_records = gold_df.count()
current_ts = datetime.utcnow().isoformat()

try:
    table.update_item(
        Key={'PK': 'PIPELINE#hf-carbon', 'SK': 'LAST_RUN'},
        UpdateExpression="SET gold_status = :s, gold_records = :r, gold_path = :p, gold_csv_path = :cp, gold_completed_at = :t, run_id = :rid",
        ExpressionAttributeValues={
            ':s': 'COMPLETED',
            ':r': total_records,
            ':p': target_path_parquet,
            ':cp': target_path_csv,
            ':t': current_ts,
            ':rid': RUN_ID
        }
    )
    print(f"Gold completo: {total_records} registros.")
except Exception as e:
    print(f"Error DynamoDB: {str(e)}")

job.commit()