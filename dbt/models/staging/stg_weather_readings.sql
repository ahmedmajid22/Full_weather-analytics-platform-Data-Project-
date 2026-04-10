WITH source AS (
    SELECT * FROM {{ source('weather', 'weather_readings') }}
),
cleaned AS (
    SELECT
        city_id,
        timestamp                                          AS reading_at,
        temperature::NUMERIC(5,2)                          AS temperature_c,
        feels_like::NUMERIC(5,2)                           AS feels_like_c,
        ROUND((temperature * 9.0/5 + 32)::NUMERIC, 2)     AS temperature_f,
        precipitation::NUMERIC(6,2)                        AS precipitation_mm,
        windspeed::NUMERIC(5,2)                            AS windspeed_kmh,
        humidity                                           AS humidity_pct,
        weather_code,
        CASE
            WHEN weather_code = 0              THEN 'Clear sky'
            WHEN weather_code IN (1,2,3)       THEN 'Partly cloudy'
            WHEN weather_code IN (45,48)       THEN 'Foggy'
            WHEN weather_code IN (51,53,55)    THEN 'Drizzle'
            WHEN weather_code IN (61,63,65)    THEN 'Rain'
            WHEN weather_code IN (71,73,75,77) THEN 'Snow'
            WHEN weather_code IN (80,81,82)    THEN 'Rain showers'
            WHEN weather_code IN (95,96,99)    THEN 'Thunderstorm'
            ELSE 'Unknown'
        END AS weather_condition,
        CASE
            WHEN temperature < -90 OR temperature > 60 THEN TRUE
            WHEN humidity < 0 OR humidity > 100        THEN TRUE
            ELSE FALSE
        END AS is_suspicious
    FROM source
    WHERE timestamp IS NOT NULL
      AND city_id IS NOT NULL
)
SELECT * FROM cleaned
WHERE NOT is_suspicious