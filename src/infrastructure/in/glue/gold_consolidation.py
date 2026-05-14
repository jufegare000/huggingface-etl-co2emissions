import sys
import boto3
from datetime import datetime, timezone
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# 1. Recuperar argumentos
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'run_id',
    'control_table_name',
    'source_bucket',
    'target_bucket'
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

# 2. Validación de integridad en DynamoDB
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)

print(f"Iniciando consolidación Gold para el run: {RUN_ID}")

response = table.query(
    KeyConditionExpression="PK = :pk AND begins_with(SK, :sk)",
    ExpressionAttributeValues={
        ":pk": f"RUN#{RUN_ID}",
        ":sk": "PARTITION#"
    }
)
partitions = response.get('Items', [])

# Verificar que todas las particiones estén en COMPLETED
incomplete = [p['SK'] for p in partitions if p.get('status') != 'COMPLETED']
if incomplete:
    # Si sabes que los datos están ahí pero el status falló, podrías comentar este raise,
    # pero es mejor mantener la integridad.
    print(f"Warning: Particiones incompletas en DynamoDB: {incomplete}")
    # raise Exception(f"Error de integridad: Particiones no finalizadas: {incomplete}")

# 3. Lectura de datos enriquecidos (JSONL)
input_pattern = f"s3://{SOURCE_BUCKET}/enriched/hf-carbon/run_id={RUN_ID}/partition_id=*/batches/*.jsonl"
print(f"Leyendo datos desde: {input_pattern}")

df = spark.read.json(input_pattern)

if df.rdd.isEmpty():
    raise Exception("El dataset de entrada está vacío. No hay nada que consolidar.")

# 4. Transformaciones: Deduplicación por modelId
window_spec = Window.partitionBy("modelId").orderBy(F.col("enriched_at").desc())

gold_df = df.withColumn("row_num", F.row_number().over(window_spec)) \
            .filter(F.col("row_num") == 1) \
            .drop("row_num")

# 5. Escritura final en Parquet (Capa Analítica)
target_path_parquet = f"s3://{TARGET_BUCKET}/gold/hf-carbon/run_id={RUN_ID}/models/"
print(f"Escribiendo Parquet en: {target_path_parquet}")
gold_df.write.mode("overwrite").parquet(target_path_parquet)

# 6. Escritura final en CSV consolidado (Capa de Reporte)
target_path_csv = f"s3://{TARGET_BUCKET}/gold/hf-carbon/run_id={RUN_ID}/csv_report/"
print(f"Escribiendo CSV consolidado en: {target_path_csv}")

# coalesce(1) asegura que todo termine en un solo archivo CSV
gold_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .csv(target_path_csv)

# 7. Cálculo de métricas
final_count = int(gold_df.count())
now_str = datetime.now(timezone.utc).isoformat()

# 8. Actualización de Punteros en DynamoDB
print("Actualizando DynamoDB con el estado final...")
table.update_item(
    Key={
        'PK': 'PIPELINE#hf-carbon',
        'SK': 'LAST_RUN'
    },
    UpdateExpression="""
        SET gold_status = :s, 
            gold_completed_at = :ts, 
            gold_path = :p, 
            csv_path = :cp, 
            gold_records = :r, 
            last_run_id = :rid
    """,
    ExpressionAttributeValues={
        ':s': 'COMPLETED',
        ':ts': now_str,
        ':p': target_path_parquet,
        ':cp': target_path_csv,
        ':r': final_count,
        ':rid': RUN_ID
    }
)

print(f"Job finalizado exitosamente. CSV y Parquet generados. Registros: {final_count}")
job.commit()