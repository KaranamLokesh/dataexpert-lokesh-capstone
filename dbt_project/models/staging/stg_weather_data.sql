{{ config(materialized='table') }}

WITH latest_weather_data AS (
    SELECT
        city,
        temperature,
        humidity,
        pressure,
        visibility,
        wind_speed,
        wind_direction,
        sunrise,
        sunset,
        weather_description,
        cloudiness,
        timestamp,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY timestamp DESC) AS row_num
    FROM {{ source('train_data', 'weather_data') }}
)

SELECT
    city,
    temperature,
    humidity,
    pressure,
    visibility,
    wind_speed,
    wind_direction,
    sunrise,
    sunset,
    weather_description,
    cloudiness,
    timestamp
FROM latest_weather_data
WHERE row_num = 1