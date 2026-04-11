{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Step 1: Aggregate raw hourly readings to one row per city per day.
-- We cannot apply a window function directly on raw readings alongside GROUP BY,
-- so we reduce to daily granularity first, then window over that result.
WITH daily_agg AS (
    SELECT
        r.city_id,
        c.name      AS city_name,
        c.continent,
        c.region,
        DATE_TRUNC('day', r.reading_at)   AS day,
        AVG(r.temperature_c)::NUMERIC     AS daily_avg_temp_c
    FROM {{ ref('stg_weather_readings') }} r
    JOIN {{ ref('stg_cities') }}           c ON c.id = r.city_id
    WHERE r.temperature_c IS NOT NULL
    GROUP BY
        r.city_id,
        c.name,
        c.continent,
        c.region,
        DATE_TRUNC('day', r.reading_at)
),

-- Step 2: Apply the 7-day rolling average window over the daily aggregation.
-- ROWS BETWEEN 6 PRECEDING AND CURRENT ROW = at most 7 rows (today + 6 prior days).
-- For cities with fewer than 7 days of data, the window uses however many days exist,
-- so rolling avg is never NULL as long as daily_avg_temp_c is not NULL.
with_rolling AS (
    SELECT
        city_id,
        city_name,
        continent,
        region,
        day,
        ROUND(daily_avg_temp_c, 2) AS daily_avg_temp_c,
        ROUND(
            AVG(daily_avg_temp_c) OVER (
                PARTITION BY city_id
                ORDER BY day
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            )::NUMERIC,
        2) AS temp_7day_rolling_avg
    FROM daily_agg
)

SELECT *
FROM with_rolling
ORDER BY city_name, day DESC