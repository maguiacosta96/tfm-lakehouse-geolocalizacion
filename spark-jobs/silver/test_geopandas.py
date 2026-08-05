import geopandas as gpd
from shapely.geometry import Point

# Cargar el GeoJSON de barrios
barrios_gdf = gpd.read_file("/opt/data/geojson/barrios.geojson", engine="pyogrio")

print(f"Barrios cargados: {len(barrios_gdf)}")
print(f"Columnas disponibles: {list(barrios_gdf.columns)}")
print(barrios_gdf[["nombre", "comuna"]].head())

# Probar point-in-polygon con una coordenada de ejemplo (CABA, cerca de Plaza de Mayo)
test_point = Point(-58.3722, -34.6083)  # (longitud, latitud) -- OJO el orden invertido en Shapely, usa Point(longitud, latitud)
match = barrios_gdf[barrios_gdf.contains(test_point)]

if not match.empty:
    print(f"El punto de prueba cae en el barrio: {match.iloc[0]['nombre']}")
else:
    print("El punto de prueba no cayó en ningún barrio")