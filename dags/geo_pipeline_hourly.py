from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Comando base de spark-submit reutilizado por todas las tareas
SPARK_SUBMIT_BASE = (
    "docker exec spark-master /opt/spark/bin/spark-submit "
    "--conf spark.jars.ivy=/tmp/.ivy2 "
)

KAFKA_PACKAGES = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
DELTA_PACKAGES = "io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"

default_args = {
    "owner": "magali",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="geo_pipeline_hourly",
    default_args=default_args,
    description="Pipeline horario: Kafka -> Bronze -> Silver -> Gold",
    schedule_interval="@hourly",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["tfm", "lakehouse"],
) as dag:

    consume_kafka_events = BashOperator(
        task_id="consume_kafka_events",
        bash_command=(
            f"{SPARK_SUBMIT_BASE} --packages {KAFKA_PACKAGES} "
            f"/opt/spark-jobs/bronze/bronze_job.py"
        ),
    )

    process_silver_layer = BashOperator(
        task_id="process_silver_layer",
        bash_command=(
            f"{SPARK_SUBMIT_BASE} --packages {DELTA_PACKAGES} "
            f"/opt/spark-jobs/silver/silver_job.py"
        ),
    )

    process_gold_layer = BashOperator(
        task_id="process_gold_layer",
        bash_command=(
            f"{SPARK_SUBMIT_BASE} --packages {DELTA_PACKAGES} "
            f"/opt/spark-jobs/gold/gold_job.py"
        ),
    )

    refresh_dashboard_data = BashOperator(
        task_id="refresh_dashboard_data",
        bash_command='echo "Placeholder: refresco de Power BI se implementará en Semana 6"',
    )

    consume_kafka_events >> process_silver_layer >> process_gold_layer >> refresh_dashboard_data