# tests/test_dbt_models.py
"""
Integration tests for dbt models.
Run after `dbt run` with: pytest tests/test_dbt_models.py
Requires POSTGRES_HOST and POSTGRES_PORT env vars (defaults: localhost:5433).
"""
import os
import pytest
import psycopg2


@pytest.fixture(scope="module")
def db_conn():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5433")),
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


def test_daily_summary_null_temp_rate(db_conn):
    """
    avg_temp_c CAN be NULL when an entire city-day had no valid readings (API outage).
    We allow up to 5% NULL rate rather than asserting zero — that would be too strict.
    """
    cursor = db_conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM weather_marts.daily_summary")
    total = cursor.fetchone()[0]

    cursor.execute("""
        SELECT COUNT(*) FROM weather_marts.daily_summary
        WHERE avg_temp_c IS NULL
    """)
    null_count = cursor.fetchone()[0]

    if total > 0:
        null_rate = null_count / total
        assert null_rate < 0.05, (
            f"NULL avg_temp_c rate is {null_rate:.1%} — exceeds 5% threshold. "
            f"Possible API outage or data quality issue."
        )


def test_city_comparisons_all_cities_present(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("SELECT COUNT(DISTINCT city_name) FROM weather_marts.city_comparisons")
    city_count = cursor.fetchone()[0]
    assert city_count >= 45, f"Expected at least 45 cities, got {city_count}"


def test_temperature_anomalies_z_score_range(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT MAX(ABS(z_score)) FROM weather_marts.temperature_anomalies
        WHERE z_score IS NOT NULL
    """)
    max_z = cursor.fetchone()[0]
    # If no rows yet (< 24h of data), mart is empty — that is expected and acceptable
    if max_z is not None:
        assert max_z < 10, f"Z-scores > 10 suggest a data quality issue: max={max_z}"


def test_extreme_weather_event_types_valid(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("SELECT DISTINCT event_type FROM weather_marts.extreme_weather")
    valid_types = {"Heatwave", "Freeze", "High winds", "Heavy rain", "Thunderstorm", "Other"}
    actual_types = {row[0] for row in cursor.fetchall()}
    unexpected = actual_types - valid_types
    assert not unexpected, f"Unexpected event_type values: {unexpected}"


def test_weekly_trends_rolling_avg_never_null_after_7_days(db_conn):
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM weather_marts.weekly_trends
        WHERE temp_7day_rolling_avg IS NULL
          AND day < NOW() - INTERVAL '7 days'
    """)
    null_count = cursor.fetchone()[0]
    assert null_count == 0, (
        "Rolling avg should not be NULL for days more than 7 days ago — "
        f"found {null_count} rows"
    )


def test_city_comparisons_has_coordinates(db_conn):
    """Latitude and longitude must be present for the Grafana geomap to render."""
    cursor = db_conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM weather_marts.city_comparisons
        WHERE latitude IS NULL OR longitude IS NULL
    """)
    missing_coords = cursor.fetchone()[0]
    assert missing_coords == 0, f"{missing_coords} cities are missing coordinates"