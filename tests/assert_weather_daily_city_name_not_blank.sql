-- Singular test: trả về các dòng có tên thành phố NULL, rỗng hoặc chỉ chứa khoảng trắng.
SELECT *
FROM {{ ref('fct_weather_daily') }}
WHERE city_name IS NULL
   OR TRIM(city_name) = ''