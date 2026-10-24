# Bài 2 — Nạp dữ liệu raw vào PostgreSQL

## Mục tiêu

Sau bài này, bạn hiểu cách đưa các file JSON vào PostgreSQL, vì sao dữ liệu gốc nên được giữ ở dạng `JSONB`, cách dùng khóa chính để tránh bản ghi trùng và cách chạy lại loader an toàn.

## Luồng dữ liệu

```text
Open-Meteo API
    -> weather_pipeline/ingest.py
    -> data/raw/YYYY-MM-DD_city-key.json
    -> weather_pipeline/load_postgres.py
    -> PostgreSQL.raw_weather
```

Mỗi file raw chứa metadata (`source`, `city_key`, `requested_date`, `fetched_at`) và payload API trong `payload`. Loader đọc tất cả file `*.json`, kiểm tra các trường chính rồi ghi metadata thành cột quan hệ; payload được lưu nguyên cấu trúc trong cột `JSONB`.

## PostgreSQL chạy bằng Docker Compose

Trong [docker-compose.yml](../../docker-compose.yml), Compose khởi chạy PostgreSQL 16 với database/user `weather`, volume bền vững `postgres_data` và host port `5433` trỏ tới port `5432` trong container.

```powershell
docker compose up -d
docker compose ps
```

DBeaver chạy trực tiếp trên Windows nên dùng:

- Host: `localhost`
- Port: `5433`
- Database: `weather`
- Username: `weather`
- Password: giá trị `POSTGRES_PASSWORD` đã cấu hình; nếu chưa override thì `weather_local_only`

Tên host `postgres` hoặc `weather-postgres` chỉ dùng khi kết nối từ một container trong mạng Compose; trong trường hợp đó port là `5432`.

## Schema và khóa chính

[sql/init.sql](../../sql/init.sql) tạo bảng `raw_weather`. Khóa chính `(city_key, observation_date)` định danh một quan sát theo thành phố và ngày. Điều này cho phép database từ chối hai hàng raw có cùng khóa.

`payload JSONB` giữ dữ liệu linh hoạt ở dạng JSON có thể truy vấn bằng toán tử PostgreSQL. Các cột riêng như `city_key` và `observation_date` giúp lọc, nối bảng và đảm bảo tính duy nhất mà không phải phân tích JSON mỗi lần.

## Nạp file và upsert

Cài dependency trong môi trường Python đã chọn:

```powershell
py -3 -m pip install -r requirements.txt
```

Nạp các file trong `data/raw`:

```powershell
py -3 -m weather_pipeline.load_postgres
```

Loader dùng `INSERT ... ON CONFLICT (city_key, observation_date) DO UPDATE`. Lần đầu, khóa mới được thêm; lần chạy sau, cùng khóa được cập nhật thay vì nhân đôi. Tính chất này thường gọi là idempotency theo khóa nghiệp vụ.

## Truy vấn ôn tập

Mở SQL shell trong container:

```powershell
docker compose exec postgres psql -U weather -d weather
```

Đếm số bản ghi theo thành phố:

```sql
SELECT city_key, COUNT(*) AS observations
FROM raw_weather
GROUP BY city_key
ORDER BY city_key;
```

Đọc nhiệt độ trung bình từ JSONB:

```sql
SELECT
    city_key,
    observation_date,
    payload->'daily'->'temperature_2m_mean'->>0 AS mean_temperature_c
FROM raw_weather
ORDER BY observation_date, city_key;
```

`->` giữ kết quả dạng JSON/JSONB; `->>` lấy giá trị thành text. Vì API trả mỗi chỉ số daily dưới dạng mảng, `->>0` đọc phần tử đầu tiên.

## Bài tập tự ôn

1. Chạy loader hai lần và so sánh `COUNT(*)` trước/sau; giải thích vì sao số hàng không tăng.
2. Đổi truy vấn nhiệt độ để cast giá trị sang `NUMERIC` rồi sắp xếp nhiệt độ giảm dần.
3. Thêm điều kiện lọc một thành phố cụ thể.
4. Xem payload của một hàng bằng `SELECT payload FROM raw_weather WHERE city_key = 'hanoi';`.

## Lệnh an toàn dữ liệu

`docker compose down` dừng và xóa container nhưng giữ volume. `docker compose down -v` còn xóa volume `postgres_data` và toàn bộ dữ liệu trong database; chỉ chạy khi chủ ý xóa dữ liệu học tập.
