-- Generic test của dbt: nhóm khóa hạt dữ liệu và trả về khóa bị lặp.
SELECT
    city_key,
    observation_date
FROM {{ ref('fct_weather_daily') }}
GROUP BY city_key, observation_date
HAVING COUNT(*) > 1
