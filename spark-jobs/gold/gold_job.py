from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, count

spark = SparkSession.builder \
    .appName("GoldLayerJob") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Leer Silver completo
silver_df = spark.read.format("delta").load("s3a://silver/geo_events_enriched")

print(f"Total de eventos leídos de Silver: {silver_df.count()}")

# Filtramos solo eventos georreferenciados (con barrio asignado) para las métricas geográficas
silver_geo_df = silver_df.filter(col("barrio").isNotNull())

# ============================================================
# TABLA 1: metrics_by_barrio
# ============================================================
metrics_by_barrio = silver_geo_df.groupBy("fecha", "barrio", "comuna") \
    .agg(
        count("event_id").alias("total_eventos"),
        countDistinct("user_id").alias("usuarios_unicos")
    ) \
    .orderBy(col("total_eventos").desc())

print("\n--- Métricas por barrio (top 10) ---")
metrics_by_barrio.show(10, truncate=False)

metrics_by_barrio.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("fecha") \
    .save("s3a://gold/metrics_by_barrio")

# ============================================================
# TABLA 2: metrics_by_segment_hour
# ============================================================
metrics_by_segment_hour = silver_geo_df.groupBy("segmento", "hora_del_dia", "barrio") \
    .agg(count("event_id").alias("total_eventos")) \
    .orderBy(col("total_eventos").desc())

print("\n--- Métricas por segmento y hora (top 10) ---")
metrics_by_segment_hour.show(10, truncate=False)

metrics_by_segment_hour.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://gold/metrics_by_segment_hour")

# ============================================================
# TABLA 3: metrics_by_age_weekday
# ============================================================
metrics_by_age_weekday = silver_geo_df.groupBy("rango_etario", "dia_semana", "barrio") \
    .agg(count("event_id").alias("total_eventos")) \
    .orderBy(col("total_eventos").desc())

print("\n--- Métricas por rango etario y día de semana (top 10) ---")
metrics_by_age_weekday.show(10, truncate=False)

metrics_by_age_weekday.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://gold/metrics_by_age_weekday")

print("\nEscritura de tablas Gold completada (metrics_by_barrio, metrics_by_segment_hour, metrics_by_age_weekday).")


spark.stop()