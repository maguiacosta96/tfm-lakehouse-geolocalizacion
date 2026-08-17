from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckGold") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

tables = [
    "metrics_by_barrio",
    "metrics_by_segment_hour",
    "metrics_by_age_weekday",
    "campaign_recommendations",
]

for table in tables:
    print(f"\n{'='*60}")
    print(f"TABLA: {table}")
    print('='*60)
    df = spark.read.format("delta").load(f"s3a://gold/{table}")
    print(f"Filas: {df.count()}")
    print(f"Columnas: {df.columns}")
    df.show(5, truncate=False)

spark.stop()