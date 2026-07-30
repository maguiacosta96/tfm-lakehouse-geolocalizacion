from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("SilverLayerJob") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 1. Leer la capa Bronze completa
bronze_df = spark.read.format("delta").load("s3a://bronze/geo_events")

print(f"Total de eventos leídos de Bronze: {bronze_df.count()}")

# 2. Validación de schema: campos requeridos no nulos
required_fields = ["event_id", "user_id", "timestamp", "latitude", "longitude"]

validation_condition = None
for field in required_fields:
    condition = col(field).isNotNull()
    validation_condition = condition if validation_condition is None else (validation_condition & condition)

valid_df = bronze_df.filter(validation_condition)
invalid_df = bronze_df.filter(~validation_condition)

valid_count = valid_df.count()
invalid_count = invalid_df.count()

print(f"Eventos válidos: {valid_count}")
print(f"Eventos inválidos (schema): {invalid_count}")

if invalid_count > 0:
    print("Ejemplos de eventos inválidos:")
    invalid_df.show(5, truncate=False)

# 3. Guardar los inválidos para análisis de calidad (capa Silver, tabla separada de "rejected")
if invalid_count > 0:
    invalid_df.write \
        .format("delta") \
        .mode("append") \
        .save("s3a://silver/rejected_schema")
    print("Eventos inválidos guardados en s3a://silver/rejected_schema")

# 4. Validación geográfica
CABA_LAT_RANGE = (-34.70, -34.53)
CABA_LON_RANGE = (-58.53, -58.35)

geo_valid_df = valid_df.filter(
    (col("latitude").between(*CABA_LAT_RANGE)) &
    (col("longitude").between(*CABA_LON_RANGE))
)
geo_invalid_df = valid_df.filter(
    ~((col("latitude").between(*CABA_LAT_RANGE)) &
      (col("longitude").between(*CABA_LON_RANGE)))
)

geo_valid_count = geo_valid_df.count()
geo_invalid_count = geo_invalid_df.count()

print(f"Eventos válidos geográficamente (dentro de CABA): {geo_valid_count}")
print(f"Eventos fuera de rango geográfico: {geo_invalid_count}")

if geo_invalid_count > 0:
    geo_invalid_df.write \
        .format("delta") \
        .mode("append") \
        .save("s3a://silver/rejected_geo")
    print("Eventos fuera de rango guardados en s3a://silver/rejected_geo")

# 5. Deduplicación por event_id
deduped_df = geo_valid_df.dropDuplicates(["event_id"])

deduped_count = deduped_df.count()
duplicates_removed = geo_valid_count - deduped_count

print(f"Eventos después de deduplicar: {deduped_count}")
print(f"Duplicados eliminados: {duplicates_removed}")

deduped_df.show(5, truncate=False)

spark.stop()