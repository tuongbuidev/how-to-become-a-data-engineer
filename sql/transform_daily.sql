-- Bảng phân tích có kiểu dữ liệu rõ ràng, một dòng cho mỗi thành phố và ngày.
CREATE TABLE IF NOT EXISTS weather_daily (
    city_key TEXT NOT NULL,
    city_name TEXT NOT NULL,
    observation_date DATE NOT NULL,
    mean_temperature_c NUMERIC(5, 2),
    precipitation_mm NUMERIC(8, 2),
    max_wind_speed_kmh NUMERIC(8, 2),
    source TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    transformed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city_key, observation_date)
);

-- Lấy các chỉ số đầu ngày từ mảng JSONB; cập nhật bản ghi cũ để chạy lại an toàn.
INSERT INTO weather_daily (
    city_key,
    city_name,
    observation_date,
    mean_temperature_c,
    precipitation_mm,
    max_wind_speed_kmh,
    source,
    fetched_at,
    transformed_at
)
SELECT
    city_key,
    COALESCE(payload->>'name', city_key) AS city_name,
    observation_date,
    (payload->'daily'->'temperature_2m_mean'->>0)::NUMERIC(5, 2),
    (payload->'daily'->'precipitation_sum'->>0)::NUMERIC(8, 2),
    (payload->'daily'->'wind_speed_10m_max'->>0)::NUMERIC(8, 2),
    source,
    fetched_at,
    NOW()
FROM raw_weather
ON CONFLICT (city_key, observation_date) DO UPDATE SET
    city_name = EXCLUDED.city_name,
    mean_temperature_c = EXCLUDED.mean_temperature_c,
    precipitation_mm = EXCLUDED.precipitation_mm,
    max_wind_speed_kmh = EXCLUDED.max_wind_speed_kmh,
    source = EXCLUDED.source,
    fetched_at = EXCLUDED.fetched_at,
    transformed_at = NOW();
