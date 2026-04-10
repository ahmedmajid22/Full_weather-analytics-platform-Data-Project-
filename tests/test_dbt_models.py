# tests/test_dbt_models.py
"""
Integration tests for dbt models.
Run after `dbt run` with: pytest tests/test_dbt_models.py
"""
import pytest
import psycopg2
import os


@pytest.fixture(scope="module")
def db_conn():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5433"),
        database="airflow",
        user="airflow",
        password="airflow",
    )
    yield conn
    conn.close()


def test_daily_summary_has_rows(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather_marts.daily_summary")
    count = cursor.fetchone()[0]
    assert count > 0, "daily_summary mart should have rows after dbt run"


def test_daily_summary_no_null_temps(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM weather_marts.daily_summary
        WHERE avg_temp_c IS NULL
    """)
    null_count = cursor.fetchone()[0]
    assert null_count == 0, "avg_temp_c should never be null in daily_summary"


def test_city_comparisons_all_cities_present(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT city_name) FROM weather_marts.city_comparisons")
    city_count = cursor.fetchone()[0]
    assert city_count >= 45, f"Expected at least 45 cities, got {city_count}"


def test_temperature_anomalies_z_score_range(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT MAX(ABS(z_score)) FROM weather_marts.temperature_anomalies
    """)
    max_z = cursor.fetchone()[0]
    assert max_z is not None
    assert max_z < 10, f"Z-scores > 10 suggest data quality issue: max={max_z}"


def test_extreme_weather_event_types_valid(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT DISTINCT event_type FROM weather_marts.extreme_weather
    """)
    valid_types = {"Heatwave", "Freeze", "High winds", "Heavy rain", "Thunderstorm", "Other"}
    actual_types = {row[0] for row in cursor.fetchall()}
    assert actual_types.issubset(valid_types), f"Unexpected event types: {actual_types - valid_types}"


def test_weekly_trends_rolling_avg_never_null(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM weather_marts.weekly_trends
        WHERE temp_7day_rolling_avg IS NULL
        AND day < NOW() - INTERVAL '7 days'
    """)
    null_count = cursor.fetchone()[0]
    assert null_count == 0, "Rolling avg should not be null after 7 days of data"