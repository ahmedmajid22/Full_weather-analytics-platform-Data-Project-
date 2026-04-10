{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH readings AS (
    SELECT
        city_id,
        temperature_c,
        windspeed_kmh,
        humidity_pct,
        precipitation_mm
    FROM {{ ref('stg_weather_readings') }}
),
cities AS (
    SELECT id, name, country, continent, region, latitude, longitude
    FROM {{ ref('stg_cities') }}
)

SELECT
    cities.name                                             AS city_name,
    cities.country,
    cities.continent,
    cities.region,
    cities.latitude,
    cities.longitude,
    ROUND(AVG(readings.temperature_c)::NUMERIC, 2)          AS avg_temp_c,
    ROUND(MIN(readings.temperature_c)::NUMERIC, 2)          AS min_temp_c,
    ROUND(MAX(readings.temperature_c)::NUMERIC, 2)          AS max_temp_c,
    ROUND(AVG(readings.windspeed_kmh)::NUMERIC, 2)          AS avg_windspeed_kmh,
    ROUND(AVG(readings.humidity_pct)::NUMERIC, 1)           AS avg_humidity_pct,
    ROUND(SUM(readings.precipitation_mm)::NUMERIC, 2)       AS total_precip_mm,
    COUNT(*)                                                AS total_readings
FROM readings
JOIN cities ON cities.id = readings.city_id
GROUP BY cities.name, cities.country, cities.continent, cities.region, cities.latitude, cities.longitude
ORDER BY avg_temp_c DESC