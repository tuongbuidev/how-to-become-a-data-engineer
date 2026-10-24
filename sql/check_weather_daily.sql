WITH checks AS (
    SELECT
        'weather_daily_has_rows' AS check_name,
        CASE WHEN EXISTS (SELECT 1 FROM weather_daily) THEN 0 ELSE 1 END AS failed_rows

    UNION ALL

    SELECT
        'all_raw_rows_are_transformed',
        COUNT(*)::INTEGER
    FROM raw_weather AS raw
    LEFT JOIN weather_daily AS daily
        USING (city_key, observation_date)
    WHERE daily.city_key IS NULL

    UNION ALL

    SELECT
        'all_daily_rows_have_raw_source',
        COUNT(*)::INTEGER
    FROM weather_daily AS daily
    LEFT JOIN raw_weather AS raw
        USING (city_key, observation_date)
    WHERE raw.city_key IS NULL

    UNION ALL

    SELECT
        'daily_metrics_are_present',
        COUNT(*)::INTEGER
    FROM weather_daily
    WHERE mean_temperature_c IS NULL
       OR precipitation_mm IS NULL
       OR max_wind_speed_kmh IS NULL

    UNION ALL

    SELECT
        'temperature_is_within_physical_range',
        COUNT(*)::INTEGER
    FROM weather_daily
    WHERE mean_temperature_c < -90
       OR mean_temperature_c > 60

    UNION ALL

    SELECT
        'precipitation_is_nonnegative',
        COUNT(*)::INTEGER
    FROM weather_daily
    WHERE precipitation_mm < 0

    UNION ALL

    SELECT
        'wind_speed_is_nonnegative',
        COUNT(*)::INTEGER
    FROM weather_daily
    WHERE max_wind_speed_kmh < 0
)
SELECT
    check_name,
    failed_rows,
    CASE WHEN failed_rows = 0 THEN 'PASS' ELSE 'FAIL' END AS status
FROM checks
ORDER BY check_name;
