# airflow/dags/weather_etl_dag.py
import json
from datetime import datetime, timedelta

import requests
import psycopg2
import boto3
from botocore.client import Config
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

OPEN_METEO_URL  = "https://api.open-meteo.com/v1/forecast"
PUSHGATEWAY_URL = "http://pushgateway:9091"
MINIO_ENDPOINT  = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "windspeed_10m",
    "relative_humidity_2m",
    "weathercode",
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def get_pg_conn():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    return hook.get_conn()


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
    """
    Fetch hourly weather for all cities from Open-Meteo.
    Stores raw JSON in MinIO (partitioned by city/year/month/day/hour).
    Pushes per-city XCom metadata so load_to_postgres can locate each file.
    Returns a summary dict — NOT the raw data (XCom size limit).
    """
    execution_dt = context["data_interval_start"]
    year  = execution_dt.strftime("%Y")
    month = execution_dt.strftime("%m")
    day   = execution_dt.strftime("%d")
    hour  = execution_dt.strftime("%H")

    conn   = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, latitude, longitude FROM weather.cities ORDER BY id")
    cities = cursor.fetchall()
    cursor.close()
    conn.close()

    s3 = get_s3_client()
    success_count = 0
    error_count   = 0

    for city_id, name, lat, lon in cities:
        try:
            params = {
                "latitude":     float(lat),
                "longitude":    float(lon),
                "hourly":       ",".join(VARIABLES),
                "timezone":     "UTC",
                "forecast_days": 1,
            }
            resp = requests.get(OPEN_METEO_URL, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            city_slug = name.replace(" ", "_").lower()
            s3_key = (
                f"city={city_slug}/year={year}/month={month}"
                f"/day={day}/hour={hour}/data.json"
            )

            s3.put_object(
                Bucket="weather-raw",
                Key=s3_key,
                Body=json.dumps(data),
                ContentType="application/json",
            )

            # Push ONLY lightweight metadata per city (not the full JSON payload)
            context["ti"].xcom_push(
                key=f"city_{city_id}",
                value={"city_id": city_id, "name": name, "s3_key": s3_key},
            )

            success_count += 1

        except Exception as e:
            error_count += 1
            print(f"ERROR fetching {name}: {e}")

    _push_metrics("fetch_stage", success_count, error_count)
    print(f"fetch_and_stage complete — success={success_count}, errors={error_count}")
    return {"success": success_count, "failed": error_count}


def load_to_postgres(**context):
    """
    Reads each city's staged JSON from MinIO using the XCom s3_key.
    Inserts all hourly rows into weather.weather_readings with ON CONFLICT DO UPDATE
    (idempotent — safe to rerun without duplicates).
    """
    ti = context["ti"]

    # Retrieve city IDs from DB so we know which XCom keys to look up
    conn   = get_pg_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT id FROM weather.cities ORDER BY id")
    city_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()

    s3 = get_s3_client()
    rows_loaded = 0
    errors      = 0

    for city_id in city_ids:
        city_meta = ti.xcom_pull(task_ids="fetch_and_stage", key=f"city_{city_id}")
        if not city_meta:
            print(f"No XCom for city_id={city_id} — skipping")
            errors += 1
            continue

        try:
            obj  = s3.get_object(Bucket="weather-raw", Key=city_meta["s3_key"])
            data = json.loads(obj["Body"].read().decode("utf-8"))

            hourly = data.get("hourly", {})
            times  = hourly.get("time", [])

            if not times:
                print(f"No hourly data for {city_meta['name']}")
                continue

            temps  = hourly.get("temperature_2m",        [])
            feels  = hourly.get("apparent_temperature",  [])
            precip = hourly.get("precipitation",         [])
            winds  = hourly.get("windspeed_10m",         [])
            humid  = hourly.get("relative_humidity_2m",  [])
            codes  = hourly.get("weathercode",           [])

            def safe_get(lst, idx):
                return lst[idx] if idx < len(lst) else None

            cursor = conn.cursor()
            for i, ts in enumerate(times):
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
                    city_id,
                    ts,
                    safe_get(temps,  i),
                    safe_get(feels,  i),
                    safe_get(precip, i),
                    safe_get(winds,  i),
                    safe_get(humid,  i),
                    safe_get(codes,  i),
                ))
                rows_loaded += 1
            conn.commit()
            cursor.close()

        except Exception as e:
            errors += 1
            print(f"ERROR loading city_id={city_id} ({city_meta.get('name')}): {e}")

    # Log this pipeline run
    run_cursor = conn.cursor()
    run_cursor.execute("""
        INSERT INTO weather.pipeline_runs
            (run_id, rows_fetched, rows_loaded, status)
        VALUES (%s, %s, %s, %s)
    """, (
        context["run_id"],
        rows_loaded + errors,
        rows_loaded,
        "success" if errors == 0 else "partial",
    ))
    conn.commit()
    run_cursor.close()
    conn.close()

    _push_metrics("load_postgres", rows_loaded, errors)
    print(f"load_to_postgres complete — rows_loaded={rows_loaded}, errors={errors}")
    return rows_loaded


def validate_data(**context):
    """
    Three data quality checks after every load:
    1. City coverage: at least 45/50 cities must have data in the last 2 hours.
    2. Null temperatures: none allowed in recent data.
    3. Extreme values: no temperature outside −90°C to 60°C.
    """
    conn   = get_pg_conn()
    cursor = conn.cursor()
    issues = []

    # Check 1: city coverage
    cursor.execute("""
        SELECT COUNT(DISTINCT city_id)
        FROM weather.weather_readings
        WHERE timestamp > NOW() - INTERVAL '2 hours'
    """)
    city_count = cursor.fetchone()[0]
    if city_count < 45:
        issues.append(f"LOW COVERAGE: only {city_count}/50 cities in last 2 hours")

    # Check 2: null temperatures in recent data
    cursor.execute("""
        SELECT COUNT(*)
        FROM weather.weather_readings
        WHERE temperature IS NULL
          AND timestamp > NOW() - INTERVAL '2 hours'
    """)
    null_count = cursor.fetchone()[0]
    if null_count > 0:
        issues.append(f"NULL TEMPERATURES: {null_count} rows in last 2 hours")

    # Check 3: physically impossible temperature values
    cursor.execute("""
        SELECT COUNT(*)
        FROM weather.weather_readings
        WHERE (temperature < -90 OR temperature > 60)
          AND timestamp > NOW() - INTERVAL '2 hours'
    """)
    extreme_count = cursor.fetchone()[0]
    if extreme_count > 0:
        issues.append(f"EXTREME VALUES: {extreme_count} rows outside −90°C to 60°C")

    cursor.close()
    conn.close()

    if issues:
        print(f"DATA QUALITY ISSUES: {issues}")
    else:
        print("All data quality checks passed ✓")

    return {"issues": issues, "passed": len(issues) == 0}


def _push_metrics(job: str, success: int, failed: int):
    try:
        registry = CollectorRegistry()
        g_ok   = Gauge("weather_rows_success_total", "Rows loaded successfully", registry=registry)
        g_fail = Gauge("weather_rows_failed_total",  "Rows failed",             registry=registry)
        g_ok.set(success)
        g_fail.set(failed)
        push_to_gateway(PUSHGATEWAY_URL, job=job, registry=registry)
    except Exception as e:
        print(f"Metrics push error (non-fatal): {e}")


with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    description="Hourly ETL: Open-Meteo → MinIO (raw JSON) → PostgreSQL → validation",
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