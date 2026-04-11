WITH source AS (

    SELECT
        city_id,
        timestamp,
        temperature,
        feels_like,
        precipitation,
        windspeed,
        humidity,
        weather_code
    FROM {{ source('weather', 'weather_readings') }}

),

cleaned AS (

    SELECT
        city_id,

        -- standardized timestamp
        timestamp::TIMESTAMP AS reading_at,

        -- temperature_c: null out suspicious values (keeps filter logic in one place)
        CASE
            WHEN temperature BETWEEN -90 AND 60 THEN temperature::NUMERIC(5,2)
            ELSE NULL
        END AS temperature_c,

        CASE
            WHEN feels_like BETWEEN -90 AND 60 THEN feels_like::NUMERIC(5,2)
            ELSE NULL
        END AS feels_like_c,

        -- BUG FIX: temperature_f must use the same validated range as temperature_c.
        -- Previously used raw `temperature`, so a value of 65°C (suspicious) would produce
        -- a valid-looking 149°F even though temperature_c was NULL for that row.
        CASE
            WHEN temperature BETWEEN -90 AND 60
                THEN ROUND((temperature * 9.0 / 5 + 32)::NUMERIC, 2)
            ELSE NULL
        END AS temperature_f,

        CASE
            WHEN precipitation >= 0 THEN precipitation::NUMERIC(6,2)
            ELSE NULL
        END AS precipitation_mm,

        CASE
            WHEN windspeed >= 0 THEN windspeed::NUMERIC(5,2)
            ELSE NULL
        END AS windspeed_kmh,

        CASE
            WHEN humidity BETWEEN 0 AND 100 THEN humidity
            ELSE NULL
        END AS humidity_pct,

        weather_code,

        CASE
            WHEN weather_code = 0              THEN 'Clear sky'
            WHEN weather_code IN (1, 2, 3)     THEN 'Cloudy'
            WHEN weather_code IN (45, 48)      THEN 'Fog'
            WHEN weather_code IN (51, 53, 55)  THEN 'Drizzle'
            WHEN weather_code IN (61, 63, 65)  THEN 'Rain'
            WHEN weather_code IN (71, 73, 75, 77) THEN 'Snow'
            WHEN weather_code IN (80, 81, 82)  THEN 'Rain showers'
            WHEN weather_code IN (95, 96, 99)  THEN 'Thunderstorm'
            ELSE 'Unknown'
        END AS weather_condition,

        -- BUG FIX: is_suspicious checks raw temperature (correct — we want to catch
        -- out-of-range source values before any CASE transforms them).
        -- NULL source temperature → ELSE FALSE → passes through with NULL temperature_c.
        -- This is intentional: missing data is not the same as bad data.
        CASE
            WHEN temperature < -90 OR temperature > 60 THEN TRUE
            WHEN humidity    < 0   OR humidity   > 100 THEN TRUE
            ELSE FALSE
        END AS is_suspicious

    FROM source

    WHERE
        timestamp  IS NOT NULL
        AND city_id IS NOT NULL

)

-- BUG FIX: removed the dead `OR is_suspicious IS NULL` branch.
-- The CASE above always resolves to TRUE or FALSE (never NULL) because of the ELSE clause,
-- so the IS NULL check never matched anything and was misleading.
SELECT *
FROM cleaned
WHERE is_suspicious = FALSE