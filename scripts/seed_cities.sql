CREATE TABLE IF NOT EXISTS weather.cities (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    latitude NUMERIC,
    longitude NUMERIC
);

INSERT INTO weather.cities (name, latitude, longitude) VALUES
('London', 51.5074, -0.1278),
('Paris', 48.8566, 2.3522),
('New York', 40.7128, -74.0060),
('Tokyo', 35.6762, 139.6503),
('Cairo', 30.0444, 31.2357);