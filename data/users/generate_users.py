import csv
from faker import Faker

fake = Faker("es_AR")  # Faker con datos localizados para Argentina

NUM_USERS = 500
SEGMENTOS = ["Premium", "Estándar", "Básico"]
SEGMENTO_WEIGHTS = [0.2, 0.5, 0.3]  # distribución realista: pocos premium, mayoría estándar

PROVINCIAS = [
    "Ciudad Autónoma de Buenos Aires", "Buenos Aires", "Córdoba",
    "Santa Fe", "Mendoza", "Tucumán", "Entre Ríos", "Salta",
]


def get_rango_etario(edad: int) -> str:
    if edad <= 30:
        return "Joven"
    elif edad <= 55:
        return "Adulto"
    else:
        return "Senior"


def generate_users(n: int) -> list[dict]:
    users = []
    for i in range(1, n + 1):
        edad = fake.random_int(min=18, max=75)
        genero = fake.random_element(elements=("Masculino", "Femenino", "No binario"))
        segmento = fake.random_element(elements=SEGMENTOS)  # ver nota abajo sobre weights

        user = {
            "user_id": f"USR_{i:05d}",
            "nombre": fake.name(),
            "edad": edad,
            "rango_etario": get_rango_etario(edad),
            "genero": genero,
            "segmento": segmento,
            "fecha_alta": fake.date_between(start_date="-3y", end_date="today").isoformat(),
            "provincia": fake.random_element(elements=PROVINCIAS),
        }
        users.append(user)
    return users


if __name__ == "__main__":
    users = generate_users(NUM_USERS)

    output_path = "users.csv"
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=users[0].keys())
        writer.writeheader()
        writer.writerows(users)

    print(f"Generados {len(users)} usuarios en '{output_path}'")
    print(f"Ejemplo: {users[0]}")