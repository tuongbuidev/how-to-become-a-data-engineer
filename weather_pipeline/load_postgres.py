import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def load_raw_files(raw_dir=RAW_DIR):
    records = []
    for path in sorted(raw_dir.glob("*.json")):
        record = json.loads(path.read_text(encoding="utf-8"))
        city_key = record.get("city_key")
        observation_date = record.get("requested_date")
        payload = record.get("payload")
        if not isinstance(city_key, str) or not isinstance(observation_date, str) or not isinstance(payload, dict):
            raise ValueError(f"Invalid raw record in {path}")
        date.fromisoformat(observation_date)
        records.append((city_key, observation_date, record["source"], record["fetched_at"], Jsonb(payload)))
    return records


def load_to_postgres(raw_dir=RAW_DIR, database_url=None):
    records = load_raw_files(raw_dir)
    if not records:
        print(f"No JSON files found in {raw_dir}")
        return 0
    database_url = database_url or os.environ.get(
        "DATABASE_URL", "postgresql://weather:weather_local_only@localhost:5433/weather"
    )
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO raw_weather (city_key, observation_date, source, fetched_at, payload)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (city_key, observation_date) DO UPDATE SET
                    source = EXCLUDED.source,
                    fetched_at = EXCLUDED.fetched_at,
                    payload = EXCLUDED.payload,
                    loaded_at = NOW()
                """,
                records,
            )
    return len(records)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Load raw weather JSON files into PostgreSQL")
    parser.add_argument("--raw-dir", type=Path, default=RAW_DIR)
    args = parser.parse_args(argv)
    try:
        count = load_to_postgres(args.raw_dir)
    except (OSError, ValueError, KeyError, psycopg.Error) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    print(f"Loaded {count} raw record(s) into PostgreSQL")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
