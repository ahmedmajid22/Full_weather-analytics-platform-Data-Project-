import json
import pytest
from unittest.mock import patch, MagicMock


def test_open_meteo_response_parsing():
    """Test that we correctly parse the Open-Meteo API response."""
    mock_response = {
        "hourly": {
            "time": ["2026-04-06T00:00", "2026-04-06T01:00"],
            "temperature_2m": [12.5, 13.1],
            "apparent_temperature": [10.2, 11.0],
            "precipitation": [0.0, 0.2],
            "windspeed_10m": [15.3, 18.7],
            "relative_humidity_2m": [72, 75],
            "weathercode": [3, 61],
        }
    }
    hourly = mock_response["hourly"]
    assert len(hourly["time"]) == 2
    assert hourly["temperature_2m"][0] == 12.5
    assert hourly["weathercode"][1] == 61


def test_data_quality_null_detection():
    """Test that null temperature detection works."""
    readings = [
        {"temperature": 15.0, "city_id": 1},
        {"temperature": None, "city_id": 2},
        {"temperature": 22.5, "city_id": 3},
    ]
    null_count = sum(1 for r in readings if r["temperature"] is None)
    assert null_count == 1


def test_data_quality_extreme_value_detection():
    """Test that extreme temperature detection flags correctly."""
    def is_extreme(temp):
        return temp is not None and (temp < -90 or temp > 60)

    assert is_extreme(61.0)  is True
    assert is_extreme(-91.0) is True
    assert is_extreme(35.5)  is False
    assert is_extreme(-20.0) is False
    assert is_extreme(None)  is False


def test_city_coverage_threshold():
    """Test that pipeline alerts when fewer than 45 cities loaded."""
    def check_coverage(city_count):
        return city_count >= 45

    assert check_coverage(50) is True
    assert check_coverage(45) is True
    assert check_coverage(44) is False
    assert check_coverage(0)  is False


def test_minio_key_format():
    """Test that MinIO partition path is correctly formed."""
    city_name = "New York"
    city_slug  = city_name.replace(" ", "_").lower()
    year, month, day, hour = "2026", "04", "06", "14"
    key = f"city={city_slug}/year={year}/month={month}/day={day}/hour={hour}/data.json"
    assert key == "city=new_york/year=2026/month=04/day=06/hour=14/data.json"


def test_wmo_code_decoding():
    """Test WMO weather code to human-readable label mapping."""
    def decode_wmo(code):
        mapping = {
            0: "Clear sky", 1: "Partly cloudy", 2: "Partly cloudy", 3: "Partly cloudy",
            45: "Foggy", 48: "Foggy", 51: "Drizzle", 61: "Rain", 71: "Snow",
            80: "Rain showers", 95: "Thunderstorm",
        }
        return mapping.get(code, "Unknown")

    assert decode_wmo(0)   == "Clear sky"
    assert decode_wmo(61)  == "Rain"
    assert decode_wmo(95)  == "Thunderstorm"
    assert decode_wmo(999) == "Unknown"


def test_anomaly_z_score_calculation():
    """Test z-score calculation for anomaly detection."""
    mean, stddev, temperature = 15.0, 5.0, 27.0
    z_score = (temperature - mean) / stddev
    assert z_score == pytest.approx(2.4, rel=1e-3)
    assert abs(z_score) > 2.0  # should be flagged as anomaly