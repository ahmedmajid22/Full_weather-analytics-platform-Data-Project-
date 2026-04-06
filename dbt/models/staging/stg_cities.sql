SELECT
    id,
    name,
    country,
    country_code,
    continent,
    latitude,
    longitude,
    timezone,
    population,
    CASE
        WHEN continent = 'Europe'        THEN 'EMEA'
        WHEN continent = 'Africa'        THEN 'EMEA'
        WHEN continent = 'Asia'          THEN 'APAC'
        WHEN continent = 'Oceania'       THEN 'APAC'
        WHEN continent = 'North America' THEN 'AMER'
        WHEN continent = 'South America' THEN 'AMER'
        ELSE 'OTHER'
    END AS region
FROM {{ source('weather', 'cities') }}