from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import geopandas as gpd
from shapely.geometry import Point
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType
import pandas as pd

# Cargar el GeoJSON una sola vez (se serializa a cada worker)
BARRIOS_GDF = gpd.read_file("/opt/data/geojson/barrios.geojson", engine="pyogrio")

@pandas_udf(StringType())
def get_barrio(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
    def find_barrio(lat, lon):
        if lat is None or lon is None:
            return None
        point = Point(lon, lat)  # OJO: Point(longitud, latitud)
        match = BARRIOS_GDF[BARRIOS_GDF.contains(point)]
        if not match.empty:
            return match.iloc[0]["nombre"]
        return None
    
    return pd.Series([find_barrio(lat, lon) for lat, lon in zip(lat_series, lon_series)])


@pandas_udf(StringType())
def get_comuna(lat_series: pd.Series, lon_series: pd.Series) -> pd.Series:
    def find_comuna(lat, lon):
        if lat is None or lon is None:
            return None
        point = Point(lon, lat)
        match = BARRIOS_GDF[BARRIOS_GDF.contains(point)]
        if not match.empty:
            return str(match.iloc[0]["comuna"])
        return None
    
    return pd.Series([find_comuna(lat, lon) for lat, lon in zip(lat_series, lon_series)])


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

from pyspark.sql.functions import to_date, hour as hour_fn, dayofweek, date_format

# 6. Normalización temporal: extraer campos derivados
silver_df = deduped_df \
    .withColumn("fecha", to_date(col("event_ts"))) \
    .withColumn("hora_del_dia", hour_fn(col("event_ts"))) \
    .withColumn("dia_semana", date_format(col("event_ts"), "EEEE"))  # nombre del día en inglés por defecto

print("Muestra con campos temporales derivados:")
silver_df.select("event_id", "event_ts", "fecha", "hora_del_dia", "dia_semana").show(5, truncate=False)
# 7. Join con dataset de usuarios
users_df = spark.read.csv(
    "/opt/data/users/users.csv",
    header=True,
    inferSchema=True
)

print(f"Total de usuarios cargados: {users_df.count()}")

silver_with_users_df = silver_df.join(users_df, on="user_id", how="left")

print("Muestra con datos de usuario incorporados:")
silver_with_users_df.select(
    "event_id", "user_id", "nombre", "segmento", "rango_etario", "fecha", "hora_del_dia"
).show(5, truncate=False)

# Verificar que no haya eventos sin match de usuario (user_id inexistente en el dataset)
unmatched_count = silver_with_users_df.filter(col("nombre").isNull()).count()
print(f"Eventos sin usuario asociado: {unmatched_count}")

# 7.5. Enriquecimiento geoespacial: asignar barrio y comuna
silver_with_geo_df = silver_with_users_df \
    .withColumn("barrio", get_barrio(col("latitude"), col("longitude"))) \
    .withColumn("comuna", get_comuna(col("latitude"), col("longitude")))

print("Muestra con barrio/comuna asignados:")
silver_with_geo_df.select("event_id", "latitude", "longitude", "barrio", "comuna").show(5, truncate=False)

# Contar eventos no georreferenciables (point-in-polygon sin match)
no_geo_count = silver_with_geo_df.filter(col("barrio").isNull()).count()
print(f"Eventos no georreferenciables: {no_geo_count}")
# 8. Escritura final en capa Silver, particionada por fecha y barrio
# (el campo "barrio" todavía no existe -- se agrega en la Tarea 14, enriquecimiento geoespacial)
# Por ahora particionamos solo por fecha
silver_with_geo_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("fecha", "barrio") \
    .save("s3a://silver/geo_events_enriched")

print("Escritura en Silver completada.")

spark.stop()