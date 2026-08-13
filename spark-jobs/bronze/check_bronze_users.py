from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckBronzeUsers") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

df = spark.read.format("delta").load("s3a://bronze/geo_events")
print(f"Total de eventos en Bronze: {df.count()}")
df.select("event_id", "user_id", "timestamp").orderBy(df.timestamp.desc()).show(20, truncate=False)

spark.stop()