import json

with open("barrios.geojson", "r", encoding="utf-8") as f:
    data = json.load(f)

features = data["features"]
print(f"Tipo de FeatureCollection: {data['type']}")
print(f"Cantidad de barrios: {len(features)}")

# Inspeccionar el primer feature
first = features[0]
print("\n--- Primer barrio ---")
print(f"Properties: {first['properties']}")
print(f"Tipo de geometría: {first['geometry']['type']}")

# Ver qué tipos de geometría aparecen en todo el dataset
geometry_types = set(f["geometry"]["type"] for f in features)
print(f"\nTipos de geometría presentes: {geometry_types}")

# Listar todos los nombres de barrios
nombres = sorted(f["properties"]["nombre"] for f in features)
print(f"\nBarrios ({len(nombres)}):")
for n in nombres:
    print(f"  - {n}")