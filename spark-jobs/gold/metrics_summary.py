from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count

spark = SparkSession.builder \
    .appName("MetricsSummary") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("="*60)
print("RESUMEN DE MÉTRICAS - TFM Acosta")
print("="*60)

# --- Bronze ---
bronze_df = spark.read.format("delta").load("s3a://bronze/geo_events")
total_bronze = bronze_df.count()
print(f"\n1. Volumen de eventos en Bronze: {total_bronze}")

# --- Silver ---
silver_df = spark.read.format("delta").load("s3a://silver/geo_events_enriched")
total_silver = silver_df.count()
print(f"2. Volumen de eventos en Silver (post-validación): {total_silver}")

pct_validos = (total_silver / total_bronze * 100) if total_bronze > 0 else 0
print(f"3. Porcentaje de registros válidos (Silver/Bronze): {pct_validos:.2f}%")

georef = silver_df.filter(col("barrio").isNotNull()).count()
no_georef = silver_df.filter(col("barrio").isNull()).count()
pct_georef = (georef / total_silver * 100) if total_silver > 0 else 0
print(f"4. Eventos georreferenciados exitosamente: {georef} ({pct_georef:.2f}%)")
print(f"   Eventos NO georreferenciables: {no_georef} ({100 - pct_georef:.2f}%)")

# --- Usuarios únicos por barrio (top 5) ---
print(f"\n5. Usuarios únicos activos por barrio (top 5):")
usuarios_por_barrio = silver_df.filter(col("barrio").isNotNull()) \
    .groupBy("barrio") \
    .agg(countDistinct("user_id").alias("usuarios_unicos")) \
    .orderBy(col("usuarios_unicos").desc())
usuarios_por_barrio.show(5, truncate=False)

# --- Gold: recomendaciones ---
campaign_df = spark.read.format("delta").load("s3a://gold/campaign_recommendations")
total_recomendaciones = campaign_df.count()
print(f"6. Número de recomendaciones de campaña generadas: {total_recomendaciones}")

print("\n" + "="*60)
print("FIN DEL RESUMEN")
print("="*60)

spark.stop()