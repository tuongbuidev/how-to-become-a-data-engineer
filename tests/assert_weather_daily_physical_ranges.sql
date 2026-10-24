SELECT *
FROM {{ ref('fct_weather_daily') }}
WHERE mean_temperature_c < -90
   OR mean_temperature_c > 60
   OR precipitation_mm < 0
   OR max_wind_speed_kmh < 0
