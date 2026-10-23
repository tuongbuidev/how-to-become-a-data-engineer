CREATE TABLE IF NOT EXISTS raw_weather (
    city_key TEXT NOT NULL,
    observation_date DATE NOT NULL,
    source TEXT NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (city_key, observation_date)
);
