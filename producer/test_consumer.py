import json
from kafka import KafkaConsumer

KAFKA_TOPIC = "geo-events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",  # lee desde el principio del tópico
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"Escuchando mensajes en '{KAFKA_TOPIC}'... (Ctrl+C para detener)")

try:
    for message in consumer:
        print(f"Recibido: {message.value}")
except KeyboardInterrupt:
    print("\nDeteniendo consumer...")
finally:
    consumer.close()