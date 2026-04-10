import logging
import psycopg2
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def check_streaming_freshness(**context):
    """Check that the Kafka consumer is writing fresh data to streaming_readings."""
    conn = psycopg2.connect(
        host="postgres", database="airflow",
        user="airflow", password="airflow"
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*), MAX(timestamp)
        FROM weather.streaming_readings
        WHERE timestamp > NOW() - INTERVAL '10 minutes'
    """)
    recent_count, latest_ts = cursor.fetchone()

    cursor.execute("""
        SELECT COUNT(DISTINCT city_id)
        FROM weather.streaming_readings
        WHERE timestamp > NOW() - INTERVAL '10 minutes'
    """)
    city_count = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    logger.info(f"Streaming: {recent_count} records, {city_count} cities, latest={latest_ts}")

    if recent_count == 0:
        raise ValueError(
            "No streaming data in last 10 minutes — "
            "check kafka-producer container: docker logs <container-name>"
        )

    return {
        "recent_count": recent_count,
        "city_count": city_count,
        "latest_timestamp": str(latest_ts),
    }


def check_alert_volume(**context):
    """Check overall streaming volume and city coverage."""
    conn = psycopg2.connect(
        host="postgres", database="airflow",
        user="airflow", password="airflow"
    )
    cursor = conn.cursor()

    cursor.execute("""
        SELECT COUNT(*) FROM weather.streaming_readings
        WHERE timestamp > NOW() - INTERVAL '1 hour'
    """)
    hour_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(DISTINCT city_id) FROM weather.streaming_readings
    """)
    total_cities = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    logger.info(f"Last hour: {hour_count} streaming records across {total_cities} cities total")
    return {"hour_count": hour_count, "total_cities": total_cities}


with DAG(
    dag_id="kafka_pipeline_monitor",
    default_args=default_args,
    description="Monitor Kafka consumer lag and streaming data freshness",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["kafka", "monitoring", "streaming"],
) as dag:

    lag_check = PythonOperator(
        task_id="check_streaming_freshness",
        python_callable=check_streaming_freshness,
    )

    alert_check = PythonOperator(
        task_id="check_alert_volume",
        python_callable=check_alert_volume,
    )

    lag_check >> alert_check