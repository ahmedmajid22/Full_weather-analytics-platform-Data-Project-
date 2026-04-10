{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH readings AS (
    SELECT
        city_id,
        reading_at,
        temperature_c,
        windspeed_kmh,
        precipitation_mm,
        weather_condition
    FROM {{ ref('stg_weather_readings') }}
    WHERE
        temperature_c > 35
        OR temperature_c < -10
        OR windspeed_kmh > 60
        OR precipitation_mm > 10
        OR weather_condition = 'Thunderstorm'
),
cities AS (
    SELECT id, name, country, continent
    FROM {{ ref('stg_cities') }}
)

SELECT
    cities.name                AS city_name,
    cities.country,
    cities.continent,
    readings.reading_at,
    readings.temperature_c,
    readings.windspeed_kmh,
    readings.precipitation_mm,
    readings.weather_condition,
    CASE
        WHEN readings.temperature_c > 35                  THEN 'Heatwave'
        WHEN readings.temperature_c < -10                 THEN 'Freeze'
        WHEN readings.windspeed_kmh > 60                  THEN 'High winds'
        WHEN readings.precipitation_mm > 10               THEN 'Heavy rain'
        WHEN readings.weather_condition = 'Thunderstorm'  THEN 'Thunderstorm'
        ELSE 'Other'
    END AS event_type
FROM readings
JOIN cities ON cities.id = readings.city_id
ORDER BY readings.reading_at DESC