SELECT
    city_key,
    city_name,
    observation_date,
    mean_temperature_c,
    precipitation_mm,
    max_wind_speed_kmh,
    source_name,
    fetched_at,
    CURRENT_TIMESTAMP AS transformed_at
FROM {{ ref('stg_weather_daily') }}
