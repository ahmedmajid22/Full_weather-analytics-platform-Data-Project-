{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH rolling_stats AS (
    SELECT
        city_id,
        AVG(temperature_c)::NUMERIC                                              AS mean_temp,
        STDDEV(temperature_c)::NUMERIC                                           AS stddev_temp,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY temperature_c)::NUMERIC     AS p25,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY temperature_c)::NUMERIC     AS p75,
        COUNT(*)                                                                  AS sample_size
    FROM {{ ref('stg_weather_readings') }}
    WHERE reading_at >= NOW() - INTERVAL '30 days'
    GROUP BY city_id
    HAVING COUNT(*) >= 24
),
scored AS (
    SELECT
        r.city_id,
        c.name             AS city_name,
        c.country,
        c.continent,
        c.region,
        r.reading_at,
        r.temperature_c,
        r.weather_condition,
        s.mean_temp,
        s.stddev_temp,
        s.sample_size,
        ROUND(
            (r.temperature_c::NUMERIC - s.mean_temp) / NULLIF(s.stddev_temp, 0),
        3) AS z_score,
        ROUND(
            (r.temperature_c::NUMERIC - s.mean_temp) / NULLIF(s.p75 - s.p25, 0),
        3) AS robust_z_score,
        CASE
            WHEN ABS((r.temperature_c::NUMERIC - s.mean_temp) / NULLIF(s.stddev_temp, 0)) > 3 THEN 'extreme'
            WHEN ABS((r.temperature_c::NUMERIC - s.mean_temp) / NULLIF(s.stddev_temp, 0)) > 2 THEN 'significant'
            ELSE 'normal'
        END AS anomaly_severity,
        ABS((r.temperature_c::NUMERIC - s.mean_temp) / NULLIF(s.stddev_temp, 0)) > 2 AS is_anomaly
    FROM {{ ref('stg_weather_readings') }}  r
    JOIN {{ ref('stg_cities') }}            c ON c.id = r.city_id
    JOIN rolling_stats                      s ON s.city_id = r.city_id
)
SELECT * FROM scored
ORDER BY ABS(z_score) DESC NULLS LAST