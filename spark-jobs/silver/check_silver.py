from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("CheckSilver") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("s3a://silver/geo_events_enriched")

print(f"Total de eventos en Silver: {df.count()}")
print("Schema:")
df.printSchema()

print("\nDistribución por barrio:")
df.groupBy("barrio").count().orderBy(col("count").desc()).show(20, truncate=False)

sin_barrio = df.filter(col("barrio").isNull()).count()
print(f"\nEventos sin barrio asignado (no georreferenciables): {sin_barrio}")

print("\nMuestra de eventos con barrio y comuna:")
df.select("event_id", "latitude", "longitude", "barrio", "comuna", "nombre", "segmento").show(10, truncate=False)

spark.stop()