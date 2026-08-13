from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CheckTestEvents") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("s3a://silver/geo_events_enriched")

# Eventos con hora nocturna (22-23 o 0-5) o día de fin de semana
sospechosos = df.filter(
    (col("hora_del_dia") >= 22) | (col("hora_del_dia") <= 5) |
    (col("dia_semana").isin("Saturday", "Sunday"))
)

print(f"Eventos candidatos a reglas (nocturno o finde): {sospechosos.count()}")
sospechosos.select(
    "event_id", "segmento", "rango_etario", "hora_del_dia", "dia_semana", "barrio"
).show(20, truncate=False)

spark.stop()