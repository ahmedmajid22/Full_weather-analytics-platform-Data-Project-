CREATE SCHEMA IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.weather_readings (
    city_id       INTEGER NOT NULL,
    timestamp     TIMESTAMPTZ NOT NULL,
    temperature   NUMERIC(5,2),
    feels_like    NUMERIC(5,2),
    precipitation NUMERIC(6,2),
    windspeed     NUMERIC(5,2),
    humidity      SMALLINT,
    weather_code  SMALLINT,
    PRIMARY KEY (city_id, timestamp)
);

CREATE INDEX idx_weather_city_ts    ON weather.weather_readings (city_id, timestamp DESC);
CREATE INDEX idx_weather_timestamp  ON weather.weather_readings (timestamp DESC);
CREATE INDEX idx_weather_code       ON weather.weather_readings (weather_code);

CREATE TABLE IF NOT EXISTS weather.pipeline_runs (
    id           SERIAL PRIMARY KEY,
    run_id       VARCHAR(200),
    status       VARCHAR(20),
    rows_loaded  INTEGER,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);