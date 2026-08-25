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

from pyspark.sql.functions import lit, when

# ============================================================
# TABLA 4: campaign_recommendations
# ============================================================

# Regla 1: Premium + horario nocturno (22-23 o 0-5) -> productos de inversión
UMBRAL_EVENTOS = 1  # umbral bajo porque el dataset es chico; ajustar con más volumen

regla_premium_nocturno = metrics_by_segment_hour.filter(
    (col("segmento") == "Premium") &
    ((col("hora_del_dia") >= 22) | (col("hora_del_dia") <= 5)) &
    (col("total_eventos") >= UMBRAL_EVENTOS)
).select(
    col("barrio"),
    lit("Premium").alias("segmento_objetivo"),
    col("hora_del_dia").cast("string").alias("franja_horaria"),
    lit("Productos de inversión").alias("campana_recomendada"),
    lit("Alta actividad Premium en horario nocturno").alias("justificacion")
)

# Regla 2: Jóvenes + fin de semana (Saturday/Sunday) -> beneficios en consumo
regla_jovenes_finde = metrics_by_age_weekday.filter(
    (col("rango_etario") == "Joven") &
    (col("dia_semana").isin("Saturday", "Sunday")) &
    (col("total_eventos") >= UMBRAL_EVENTOS)
).select(
    col("barrio"),
    lit("Joven").alias("segmento_objetivo"),
    col("dia_semana").alias("franja_horaria"),
    lit("Beneficios en consumo").alias("campana_recomendada"),
    lit("Alta actividad de jóvenes en fin de semana").alias("justificacion")
)

campaign_recommendations = regla_premium_nocturno.unionByName(regla_jovenes_finde)

print(f"\n--- Recomendaciones de campaña generadas: {campaign_recommendations.count()} ---")
campaign_recommendations.show(20, truncate=False)

campaign_recommendations.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3a://gold/campaign_recommendations")

print("Escritura de campaign_recommendations completada.")

# ============================================================
# Exportación a CSV para consumo en Power BI
# ============================================================
EXPORT_PATH = "/opt/data/../dashboards/exports"  # ajustar según montaje

metrics_by_barrio.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{EXPORT_PATH}/metrics_by_barrio_csv")
metrics_by_segment_hour.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{EXPORT_PATH}/metrics_by_segment_hour_csv")
metrics_by_age_weekday.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{EXPORT_PATH}/metrics_by_age_weekday_csv")
campaign_recommendations.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{EXPORT_PATH}/campaign_recommendations_csv")

print("Exportación CSV para Power BI completada.")
spark.stop()