import argparse
import json
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CITIES_FILE = PROJECT_ROOT / "config" / "cities.json"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
API_URL = "https://archive-api.open-meteo.com/v1/archive"


def load_cities(path=CITIES_FILE):
    with path.open(encoding="utf-8") as file:
        cities = json.load(file)
    if not isinstance(cities, dict) or not cities:
        raise ValueError("City configuration must be a non-empty JSON object")
    for key, city in cities.items():
        if not re.fullmatch(r"[a-z0-9-]+", key):
            raise ValueError(f"Invalid city key: {key!r}")
        if not isinstance(city, dict) or not isinstance(city.get("name"), str):
            raise ValueError(f"City {key!r} must have a name")
        for coordinate in ("latitude", "longitude"):
            if not isinstance(city.get(coordinate), (int, float)):
                raise ValueError(f"City {key!r} must have numeric {coordinate}")
    return cities


def build_request_url(city, requested_date):
    params = urlencode(
        {
            "latitude": city["latitude"],
            "longitude": city["longitude"],
            "start_date": requested_date.isoformat(),
            "end_date": requested_date.isoformat(),
            "daily": "temperature_2m_mean,precipitation_sum,wind_speed_10m_max",
            "timezone": "UTC",
        }
    )
    return f"{API_URL}?{params}"


def fetch_weather(city, requested_date):
    url = build_request_url(city, requested_date)
    try:
        with urlopen(url, timeout=30) as response:
            payload = json.load(response)
    except HTTPError as error:
        raise RuntimeError(f"Weather API returned HTTP {error.code}") from error
    except (URLError, TimeoutError) as error:
        raise RuntimeError(f"Could not reach weather API: {error}") from error
    except json.JSONDecodeError as error:
        raise RuntimeError("Weather API returned invalid JSON") from error

    if not isinstance(payload, dict) or "daily" not in payload:
        detail = payload.get("reason", "missing daily data") if isinstance(payload, dict) else "invalid response"
        raise RuntimeError(f"Weather API response error: {detail}")
    return payload


def save_raw(city_key, requested_date, payload, output_dir=RAW_DIR):
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{requested_date.isoformat()}_{city_key}.json"
    record = {
        "source": "open-meteo-archive",
        "city_key": city_key,
        "city_name": payload.get("name"),
        "requested_date": requested_date.isoformat(),
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }
    output_path.write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return output_path


def ingest(city_key, requested_date, cities=None, output_dir=RAW_DIR):
    cities = load_cities() if cities is None else cities
    if city_key not in cities:
        raise ValueError(f"Unknown city {city_key!r}; configure it in {CITIES_FILE}")
    payload = fetch_weather(cities[city_key], requested_date)
    payload["name"] = cities[city_key]["name"]
    return save_raw(city_key, requested_date, payload, output_dir)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Fetch one day of weather into raw JSON")
    parser.add_argument("--city", choices=sorted(load_cities()), default="hanoi")
    parser.add_argument("--date", help="Date to fetch in YYYY-MM-DD format (default: yesterday UTC)")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        requested_date = date.fromisoformat(args.date) if args.date else datetime.now(timezone.utc).date() - timedelta(days=1)
        if requested_date >= datetime.now(timezone.utc).date():
            raise ValueError("Choose a date before today; the archive contains historical data")
        output_path = ingest(args.city, requested_date)
    except (ValueError, RuntimeError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    print(f"Saved raw weather data to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
