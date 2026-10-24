SELECT
    city_key,
    observation_date
FROM {{ ref('fct_weather_daily') }}
GROUP BY city_key, observation_date
HAVING COUNT(*) > 1
