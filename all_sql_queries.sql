CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    country VARCHAR(100),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    observation_time TIMESTAMP,
    temp_c DOUBLE PRECISION,
    feelslike_c DOUBLE PRECISION,
    wind_kph DOUBLE PRECISION,
    wind_dir VARCHAR(5),
    humidity INT,
    cloud INT,
    is_day INT,
    weather_description VARCHAR(255)
);