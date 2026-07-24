import json
import uuid
import random
import time
import csv
from datetime import datetime, timezone
from kafka import KafkaProducer

LAT_RANGE = (-34.70, -34.53)
LON_RANGE = (-58.53, -58.35)
DEVICE_TYPES = ["iOS", "Android"]
APP_VERSION = "3.2.1"
KAFKA_TOPIC = "geo-events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

HOURLY_WEIGHTS = {
    0: 0.1, 1: 0.05, 2: 0.05, 3: 0.05, 4: 0.05, 5: 0.1,
    6: 0.3, 7: 0.6, 8: 0.9, 9: 1.0,
    10: 0.8, 11: 0.8, 12: 0.9, 13: 0.9, 14: 0.8, 15: 0.8, 16: 0.8, 17: 0.9,
    18: 1.0, 19: 1.0, 20: 0.9, 21: 0.7,
    22: 0.4, 23: 0.2,
}


def generate_event(user_ids: list[str]) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(user_ids),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": round(random.uniform(*LAT_RANGE), 6),
        "longitude": round(random.uniform(*LON_RANGE), 6),
        "device_type": random.choice(DEVICE_TYPES),
        "app_version": APP_VERSION,
    }


def get_current_weight() -> float:
    current_hour = datetime.now(timezone.utc).hour
    return HOURLY_WEIGHTS[current_hour]


def get_sleep_interval(base_interval: float = 2.0) -> float:
    weight = get_current_weight()
    return base_interval / weight


def load_user_ids(csv_path: str = "../data/users/users.csv") -> list[str]:
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [row["user_id"] for row in reader]


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    user_ids = load_user_ids()
    print(f"Cargados {len(user_ids)} user_ids desde el dataset de usuarios")
    print(f"Publicando eventos en el tópico '{KAFKA_TOPIC}'... (Ctrl+C para detener)")

    try:
        while True:
            event = generate_event(user_ids)
            producer.send(KAFKA_TOPIC, value=event)
            print(f"Enviado: {event}")

            sleep_time = get_sleep_interval()
            time.sleep(sleep_time)
    except KeyboardInterrupt:
        print("\nDeteniendo productor...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()