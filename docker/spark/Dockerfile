FROM apache/spark:3.5.0

USER root

RUN pip install --no-cache-dir \
    geopandas \
    shapely \
    pyogrio \
    pyarrow

USER spark