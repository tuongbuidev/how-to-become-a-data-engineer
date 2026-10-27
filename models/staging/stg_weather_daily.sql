-- Staging chỉ đổi tên và ép kiểu các trường cần dùng từ payload raw.
WITH source AS (
    SELECT *
    FROM {{ source('weather', 'raw_weather') }}
),
renamed AS (
    SELECT
        city_key,
        COALESCE(payload->>'name', city_key) AS city_name,
        observation_date,
        (payload->'daily'->'temperature_2m_mean'->>0)::NUMERIC(5, 2)
            AS mean_temperature_c,
        (payload->'daily'->'precipitation_sum'->>0)::NUMERIC(8, 2)
            AS precipitation_mm,
        (payload->'daily'->'wind_speed_10m_max'->>0)::NUMERIC(8, 2)
            AS max_wind_speed_kmh,
        source AS source_name,
        fetched_at,
        loaded_at
    FROM source
)
SELECT *
FROM renamed
