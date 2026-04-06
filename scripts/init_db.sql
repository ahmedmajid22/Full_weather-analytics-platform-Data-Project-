CREATE SCHEMA IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.cities (
    id           SERIAL PRIMARY KEY,
    name         VARCHAR(100) NOT NULL,
    country      VARCHAR(100) NOT NULL,
    country_code CHAR(2)      NOT NULL,
    continent    VARCHAR(50)  NOT NULL,
    latitude     NUMERIC(8,5) NOT NULL,
    longitude    NUMERIC(8,5) NOT NULL,
    timezone     VARCHAR(50)  NOT NULL,
    population   INTEGER
);

CREATE TABLE IF NOT EXISTS weather.weather_readings (
    city_id       INTEGER      NOT NULL,
    timestamp     TIMESTAMPTZ  NOT NULL,
    temperature   NUMERIC(5,2),
    feels_like    NUMERIC(5,2),
    precipitation NUMERIC(6,2),
    windspeed     NUMERIC(5,2),
    humidity      SMALLINT,
    weather_code  SMALLINT,
    PRIMARY KEY (city_id, timestamp),
    FOREIGN KEY (city_id) REFERENCES weather.cities(id)
);

CREATE TABLE IF NOT EXISTS weather.streaming_readings (
    city_id       INTEGER     NOT NULL,
    timestamp     TIMESTAMPTZ NOT NULL,
    temperature   NUMERIC(5,2),
    windspeed     NUMERIC(5,2),
    weather_code  SMALLINT,
    PRIMARY KEY (city_id, timestamp)
);

CREATE TABLE IF NOT EXISTS weather.pipeline_runs (
    id           SERIAL PRIMARY KEY,
    run_id       VARCHAR(200),
    city_id      INTEGER REFERENCES weather.cities(id),
    rows_fetched INTEGER,
    rows_loaded  INTEGER,
    status       VARCHAR(20) CHECK (status IN ('success', 'partial', 'failed')),
    duration_ms  INTEGER,
    error_msg    TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_weather_city_ts   ON weather.weather_readings (city_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_weather_timestamp ON weather.weather_readings (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_weather_code      ON weather.weather_readings (weather_code);
CREATE INDEX IF NOT EXISTS idx_pipeline_status   ON weather.pipeline_runs (status, created_at DESC);