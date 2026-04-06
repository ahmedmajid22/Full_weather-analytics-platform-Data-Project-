SELECT
    c.name          AS city_name,
    c.country,
    c.continent,
    DATE(r.reading_at) AS date,
    ROUND(AVG(r.temperature_c)::NUMERIC, 2)    AS avg_temp_c,
    ROUND(MIN(r.temperature_c)::NUMERIC, 2)    AS min_temp_c,
    ROUND(MAX(r.temperature_c)::NUMERIC, 2)    AS max_temp_c,
    ROUND(SUM(r.precipitation_mm)::NUMERIC, 2) AS total_precip_mm,
    ROUND(AVG(r.windspeed_kmh)::NUMERIC, 2)    AS avg_windspeed,
    ROUND(AVG(r.humidity_pct)::NUMERIC, 1)     AS avg_humidity,
    MODE() WITHIN GROUP (ORDER BY r.weather_condition) AS dominant_condition,
    COUNT(*) AS reading_count
FROM {{ ref('stg_weather_readings') }} r
JOIN {{ ref('stg_cities') }}           c ON c.id = r.city_id
GROUP BY c.name, c.country, c.continent, DATE(r.reading_at)
ORDER BY date DESC, city_name