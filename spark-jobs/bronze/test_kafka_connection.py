from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestKafkaConnection") \
    .getOrCreate()

# Lectura BATCH (no streaming) de todos los mensajes disponibles en el tópico
df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "geo-events") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

print(f"Cantidad de mensajes leídos: {df.count()}")
df.select("value").show(5, truncate=False)

spark.stop()