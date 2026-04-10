{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Step 1: aggregate to daily averages first (can't use raw columns + GROUP BY + window together)
WITH daily_agg AS (
    SELECT
        r.city_id,
        c.name       AS city_name,
        c.continent,
        c.region,
        DATE_TRUNC('day', r.reading_at)         AS day,
        AVG(r.temperature_c)::NUMERIC           AS daily_avg_temp_c
    FROM {{ ref('stg_weather_readings') }} r
    JOIN {{ ref('stg_cities') }}           c ON c.id = r.city_id
    GROUP BY r.city_id, c.name, c.continent, c.region, DATE_TRUNC('day', r.reading_at)
),

-- Step 2: apply window function on top of the aggregated result
with_rolling AS (
    SELECT
        city_id,      -- explicitly include city_id so PARTITION BY can reference it cleanly
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

SELECT * FROM with_rolling
ORDER BY city_name, day DESC