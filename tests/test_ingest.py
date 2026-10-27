import json
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from weather_pipeline.ingest import load_cities, save_raw


# Bộ kiểm thử tập trung vào cấu hình và việc lưu payload, không gọi API thật.
class IngestTests(unittest.TestCase):
    def test_loads_configured_learning_cities(self):
        cities = load_cities()
        self.assertEqual(
            set(cities),
            {"hanoi", "ho-chi-minh-city", "da-nang", "hue", "can-tho"},
        )

    def test_rejects_invalid_city_key(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            config = Path(temporary_dir) / "cities.json"
            config.write_text(json.dumps({"Hanoi!": {"name": "Hanoi", "latitude": 21, "longitude": 105}}))
            with self.assertRaisesRegex(ValueError, "Invalid city key"):
                load_cities(config)

    def test_save_raw_uses_city_and_date_and_preserves_payload(self):
        with tempfile.TemporaryDirectory() as temporary_dir:
            payload = {"latitude": 21.0, "daily": {"time": ["2026-10-21"]}}
            output_path = save_raw("hanoi", date(2026, 10, 21), payload, Path(temporary_dir))
            saved = json.loads(output_path.read_text(encoding="utf-8"))
            self.assertEqual(output_path.name, "2026-10-21_hanoi.json")
            self.assertEqual(saved["payload"], payload)
            self.assertEqual(saved["source"], "open-meteo-archive")

    @patch("weather_pipeline.ingest.urlopen")
    def test_fetch_rejects_api_error_payload(self, mock_urlopen):
        from io import BytesIO
        from weather_pipeline.ingest import fetch_weather

        mock_urlopen.return_value.__enter__.return_value = BytesIO(b'{"reason":"date unavailable"}')
        with self.assertRaisesRegex(RuntimeError, "date unavailable"):
            fetch_weather({"latitude": 21, "longitude": 105}, date(2026, 10, 21))


if __name__ == "__main__":
    unittest.main()
