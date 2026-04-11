{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- BUG FIX (primary cause of empty Grafana dashboards):
-- The previous HAVING COUNT(*) >= 168 required 7 full days of hourly data per city.
-- Any project running less than 7 days returned ZERO rows from rolling_stats,
-- causing the entire mart — and every Grafana panel querying it — to show nothing.
--
-- Changed to HAVING COUNT(*) >= 24 (one full day = 24 hourly readings).
-- This is the minimum sample size for a meaningful standard deviation.
-- Cities with less than one day of data are intentionally excluded to avoid
-- misleading z-scores from tiny samples.
--
-- Once you have 30+ days of data you may increase this threshold.

WITH rolling_stats AS (

    SELECT
        city_id,
        AVG(temperature_c)::NUMERIC                                              AS mean_temp,
        STDDEV(temperature_c)::NUMERIC                                           AS stddev_temp,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY temperature_c)::NUMERIC     AS p25,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY temperature_c)::NUMERIC     AS p75,
        COUNT(*)                                                                 AS sample_size
    FROM {{ ref('stg_weather_readings') }}
    WHERE reading_at >= NOW() - INTERVAL '30 days'
      AND temperature_c IS NOT NULL
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

        -- Standard z-score: how many σ from the 30-day mean.
        -- NULLIF guards against division by zero when all readings are identical
        -- (zero stddev). In that case z_score is NULL, not an error.
        ROUND(
            (r.temperature_c - s.mean_temp) / NULLIF(s.stddev_temp, 0),
        3) AS z_score,

        -- Robust z-score using IQR: less sensitive to extreme outliers than
        -- standard z-score. Useful when the city had one unusual reading that
        -- inflated the stddev and masked subsequent anomalies.
        -- NULL when IQR = 0 (all readings in same quartile range).
        ROUND(
            (r.temperature_c - s.mean_temp) / NULLIF(s.p75 - s.p25, 0),
        3) AS robust_z_score,

        CASE
            WHEN ABS((r.temperature_c - s.mean_temp) / NULLIF(s.stddev_temp, 0)) > 3 THEN 'extreme'
            WHEN ABS((r.temperature_c - s.mean_temp) / NULLIF(s.stddev_temp, 0)) > 2 THEN 'significant'
            ELSE 'normal'
        END AS anomaly_severity,

        ABS((r.temperature_c - s.mean_temp) / NULLIF(s.stddev_temp, 0)) > 2 AS is_anomaly

    FROM {{ ref('stg_weather_readings') }}  r
    JOIN {{ ref('stg_cities') }}            c ON c.id = r.city_id
    JOIN rolling_stats                      s ON s.city_id = r.city_id
    WHERE r.temperature_c IS NOT NULL

)

SELECT *
FROM scored
ORDER BY ABS(z_score) DESC NULLS LAST