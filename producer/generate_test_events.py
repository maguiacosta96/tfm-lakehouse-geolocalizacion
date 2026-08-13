import json
import uuid
import csv
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

LAT_RANGE = (-34.70, -34.53)
LON_RANGE = (-58.53, -58.35)
DEVICE_TYPES = ["iOS", "Android"]
APP_VERSION = "3.2.1"
KAFKA_TOPIC = "geo-events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


def load_users(csv_path: str = "../data/users/users.csv") -> list[dict]:
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def next_weekday(target_weekday: int, base: datetime) -> datetime:
    """target_weekday: 0=Monday ... 5=Saturday, 6=Sunday"""
    days_ahead = (target_weekday - base.weekday() + 7) % 7
    days_ahead = days_ahead or 7  # si hoy es el día, tomar el próximo, no hoy
    return base + timedelta(days=days_ahead)


def generate_event(user_id: str, forced_timestamp: datetime) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": forced_timestamp.isoformat(),
        "latitude": round(random.uniform(*LAT_RANGE), 6),
        "longitude": round(random.uniform(*LON_RANGE), 6),
        "device_type": random.choice(DEVICE_TYPES),
        "app_version": APP_VERSION,
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    users = load_users()
    premium_users = [u["user_id"] for u in users if u["segmento"] == "Premium"]
    joven_users = [u["user_id"] for u in users if u["rango_etario"] == "Joven"]

    print(f"Usuarios Premium disponibles: {len(premium_users)}")
    print(f"Usuarios Jóvenes disponibles: {len(joven_users)}")

    now = datetime.now(timezone.utc)
    events_to_send = []

    # --- Regla 1: Premium + horario nocturno (23hs de un día cualquiera próximo) ---
    next_day = now + timedelta(days=1)
    nighttime = next_day.replace(hour=23, minute=15, second=0, microsecond=0)
    for user_id in random.sample(premium_users, min(5, len(premium_users))):
        events_to_send.append(generate_event(user_id, nighttime))

    # --- Regla 2: Joven + fin de semana (próximo sábado, 16hs) ---
    next_saturday = next_weekday(5, now)  # 5 = sábado
    weekend_time = next_saturday.replace(hour=16, minute=0, second=0, microsecond=0)
    for user_id in random.sample(joven_users, min(5, len(joven_users))):
        events_to_send.append(generate_event(user_id, weekend_time))

    print(f"\nTotal de eventos de prueba a enviar: {len(events_to_send)}")
    print(f"Timestamp nocturno usado: {nighttime.isoformat()}")
    print(f"Timestamp fin de semana usado: {weekend_time.isoformat()} (día: {weekend_time.strftime('%A')})")

    for event in events_to_send:
        producer.send(KAFKA_TOPIC, value=event)
        print(f"Enviado: {event['user_id']} @ {event['timestamp']}")

    producer.flush()
    producer.close()
    print("\nEventos de prueba publicados en Kafka.")


if __name__ == "__main__":
    main()