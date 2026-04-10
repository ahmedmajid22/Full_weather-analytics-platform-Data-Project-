import json
import time
from datetime import datetime, timedelta

import requests
import psycopg2
import boto3
from botocore.client import Config
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from airflow import DAG
from airflow.operators.python import PythonOperator

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
PUSHGATEWAY_URL = "http://pushgateway:9091"
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

VARIABLES = [
    "temperature_2m", "apparent_temperature", "precipitation",
    "windspeed_10m", "relative_humidity_2m", "weathercode",
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def get_pg_conn():
    return psycopg2.connect(
        host="postgres", database="airflow",
        user="airflow", password="airflow"
    )


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def fetch_and_stage(**context):
    """Fetch weather for all 50 cities, stage raw JSON to MinIO."""
    execution_dt = context["execution_date"]
    year  = execution_dt.strftime("%Y")
    month = execution_dt.strftime("%m")
    day   = execution_dt.strftime("%d")
    hour  = execution_dt.strftime("%H")

    conn = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, latitude, longitude FROM weather.cities ORDER BY id")
    cities = cursor.fetchall()
    cursor.close()
    conn.close()

    s3 = get_s3_client()
    results = []
    errors  = []

    for city_id, name, lat, lon in cities:
        try:
            params = {
                "latitude": float(lat),
                "longitude": float(lon),
                "hourly": ",".join(VARIABLES),
                "timezone": "UTC",
                "forecast_days": 1,
            }
            resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            # Stage to MinIO with partitioned path
            city_slug = name.replace(" ", "_").lower()
            key = f"city={city_slug}/year={year}/month={month}/day={day}/hour={hour}/data.json"
            s3.put_object(
                Bucket="weather-raw",
                Key=key,
                Body=json.dumps(data),
                ContentType="application/json",
            )

            results.append({"city_id": city_id, "name": name, "data": data})

        except Exception as e:
            errors.append({"city": name, "error": str(e)})
            print(f"ERROR fetching {name}: {e}")

    print(f"Fetched {len(results)} cities, {len(errors)} errors")
    _push_metrics("fetch_stage", len(results), len(errors))
    return results


def load_to_postgres(**context):
    """Load fetched data into PostgreSQL with idempotent upsert."""
    ti      = context["ti"]
    results = ti.xcom_pull(task_ids="fetch_and_stage")

    if not results:
        raise ValueError("No data received from fetch_and_stage task")

    conn   = get_pg_conn()
    cursor = conn.cursor()
    run_id = context["run_id"]
    rows_loaded = 0
    rows_fetched = 0

    for city_result in results:
        city_id = city_result["city_id"]
        hourly  = city_result["data"].get("hourly", {})
        times   = hourly.get("time", [])
        rows_fetched += len(times)

        for i, ts in enumerate(times):
            try:
                cursor.execute("""
                    INSERT INTO weather.weather_readings
                        (city_id, timestamp, temperature, feels_like,
                         precipitation, windspeed, humidity, weather_code)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (city_id, timestamp) DO UPDATE SET
                        temperature   = EXCLUDED.temperature,
                        feels_like    = EXCLUDED.feels_like,
                        precipitation = EXCLUDED.precipitation,
                        windspeed     = EXCLUDED.windspeed,
                        humidity      = EXCLUDED.humidity,
                        weather_code  = EXCLUDED.weather_code
                """, (
                    city_id, ts,
                    hourly.get("temperature_2m",      [None])[i],
                    hourly.get("apparent_temperature", [None])[i],
                    hourly.get("precipitation",        [None])[i],
                    hourly.get("windspeed_10m",        [None])[i],
                    hourly.get("relative_humidity_2m", [None])[i],
                    hourly.get("weathercode",          [None])[i],
                ))
                rows_loaded += 1
            except Exception as e:
                print(f"Row error city_id={city_id} ts={ts}: {e}")

    # Log this pipeline run with both rows_fetched and rows_loaded
    cursor.execute("""
        INSERT INTO weather.pipeline_runs (run_id, rows_fetched, rows_loaded, status)
        VALUES (%s, %s, %s, %s)
    """, (run_id, rows_fetched, rows_loaded, "success"))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Fetched {rows_fetched} records, loaded {rows_loaded} rows to PostgreSQL")
    _push_metrics("load_postgres", rows_loaded, 0)
    return rows_loaded


def validate_data(**context):
    """Run 3 data quality checks and flag issues."""
    conn   = get_pg_conn()
    cursor = conn.cursor()
    issues = []

    # Check 1: city coverage in last 2 hours
    cursor.execute("""
        SELECT COUNT(DISTINCT city_id) FROM weather.weather_readings
        WHERE timestamp > NOW() - INTERVAL '2 hours'
    """)
    city_count = cursor.fetchone()[0]
    if city_count < 45:
        issues.append(f"LOW COVERAGE: only {city_count}/50 cities loaded in last 2 hours")

    # Check 2: null temperature rate
    cursor.execute("""
        SELECT COUNT(*) FROM weather.weather_readings
        WHERE temperature IS NULL
        AND timestamp > NOW() - INTERVAL '2 hours'
    """)
    null_count = cursor.fetchone()[0]
    if null_count > 0:
        issues.append(f"NULL TEMPERATURES: {null_count} readings missing temperature")

    # Check 3: extreme/impossible values
    cursor.execute("""
        SELECT COUNT(*) FROM weather.weather_readings
        WHERE (temperature < -90 OR temperature > 60)
        AND timestamp > NOW() - INTERVAL '2 hours'
    """)
    extreme_count = cursor.fetchone()[0]
    if extreme_count > 0:
        issues.append(f"EXTREME VALUES: {extreme_count} temperatures outside -90°C to 60°C")

    cursor.close()
    conn.close()

    if issues:
        print(f"DATA QUALITY ISSUES DETECTED: {issues}")
    else:
        print("All data quality checks passed.")

    return {"issues": issues, "city_coverage": city_count, "passed": len(issues) == 0}


def _push_metrics(job: str, rows_success: int, rows_failed: int):
    try:
        registry = CollectorRegistry()
        g_ok  = Gauge("weather_rows_loaded_total",  "Rows loaded",  registry=registry)
        g_err = Gauge("weather_rows_failed_total",  "Rows failed",  registry=registry)
        g_ok.set(rows_success)
        g_err.set(rows_failed)
        push_to_gateway(PUSHGATEWAY_URL, job=job, registry=registry)
    except Exception as e:
        print(f"Prometheus push failed (non-critical): {e}")


with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    description="Hourly ETL: Open-Meteo API → MinIO (raw JSON) → PostgreSQL",
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "etl", "production"],
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_and_stage",
        python_callable=fetch_and_stage,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    fetch_task >> load_task >> validate_task