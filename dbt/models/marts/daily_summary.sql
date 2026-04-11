{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH base AS (
    SELECT
        r.city_id,
        r.reading_at,
        r.temperature_c,
        r.feels_like_c,
        r.precipitation_mm,
        r.windspeed_kmh,
        r.humidity_pct,
        r.weather_condition
    FROM {{ ref('stg_weather_readings') }} r
),

cities AS (
    SELECT id, name, country, continent, region
    FROM {{ ref('stg_cities') }}
)

SELECT
    cities.name                                                        AS city_name,
    cities.country,
    cities.continent,
    cities.region,
    DATE(base.reading_at)                                              AS date,

    -- AVG/MIN/MAX skip NULLs automatically, so cities with some missing readings
    -- still get meaningful daily stats from the readings that did arrive.
    ROUND(AVG(base.temperature_c)::NUMERIC,   2)                      AS avg_temp_c,
    ROUND(MIN(base.temperature_c)::NUMERIC,   2)                      AS min_temp_c,
    ROUND(MAX(base.temperature_c)::NUMERIC,   2)                      AS max_temp_c,
    ROUND(SUM(base.precipitation_mm)::NUMERIC, 2)                     AS total_precip_mm,
    ROUND(AVG(base.windspeed_kmh)::NUMERIC,   2)                      AS avg_windspeed_kmh,
    ROUND(AVG(base.humidity_pct)::NUMERIC,    1)                      AS avg_humidity_pct,

    -- dominant_condition: most frequent weather condition label for the day.
    -- MODE() is a PostgreSQL ordered-set aggregate — safe for PG 9.4+.
    MODE() WITHIN GROUP (ORDER BY base.weather_condition)             AS dominant_condition,

    COUNT(*)                                                           AS reading_count

FROM base
JOIN cities ON cities.id = base.city_id
GROUP BY
    cities.name,
    cities.country,
    cities.continent,
    cities.region,
    DATE(base.reading_at)
ORDER BY date DESC, city_name