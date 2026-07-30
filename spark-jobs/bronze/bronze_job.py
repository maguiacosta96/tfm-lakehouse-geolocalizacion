from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, year, month, dayofmonth, hour, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True), 
    StructField("timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("device_type", StringType(), True),
    StructField("app_version", StringType(), True),
])

spark = SparkSession.builder \
    .appName("BronzeLayerJob") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

raw_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "geo-events") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

parsed_df = raw_df.select(
    from_json(col("value").cast("string"), event_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("data.*", "kafka_timestamp")

bronze_df = parsed_df \
    .withColumn("event_ts", to_timestamp(col("timestamp"))) \
    .withColumn("year", year(col("event_ts"))) \
    .withColumn("month", month(col("event_ts"))) \
    .withColumn("day", dayofmonth(col("event_ts"))) \
    .withColumn("hour", hour(col("event_ts")))

print(f"Total de eventos a escribir en Bronze: {bronze_df.count()}")
bronze_df.show(5, truncate=False)

bronze_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("year", "month", "day", "hour") \
    .save("s3a://bronze/geo_events")

print("Escritura en Bronze completada.")
spark.stop()