# Airflow local learning stack

This is a local learning setup for Apache Airflow 3.3.2, not a production deployment. The Airflow metadata database is isolated from the existing weather database. The web UI binds to `127.0.0.1:8080` only.

## Prerequisites

- Docker Desktop and Docker Compose v2
- The existing weather PostgreSQL service running from the repository root on port `5433`
- The actual password for PostgreSQL role `weather`

From the repository root, start the existing weather database if needed:

```powershell
docker compose up -d postgres
```

Create a local Airflow environment file. Generate two random values for the Airflow metadata password and API secret key, then set the real weather database password. Do not commit `.env`.

```powershell
Copy-Item airflow\.env.example airflow\.env
notepad airflow\.env
```

Example ways to generate local random values in PowerShell:

```powershell
[guid]::NewGuid().ToString('N')
$bytes = New-Object byte[] 32
[Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
[Convert]::ToBase64String($bytes)
```

The `.env` file contains `AIRFLOW_DB_PASSWORD`, `AIRFLOW_SECRET_KEY`, `AIRFLOW_JWT_SECRET`, and `WEATHER_DB_PASSWORD`. Use distinct random values for the two Airflow secrets.  Airflow's Simple Auth Manager creates the local `admin` user password on first startup; find it in `airflow\runtime\logs\simple_auth_manager_passwords.json.generated`.

## Build and start

Run these commands from the repository root:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml config --quiet
docker compose --env-file airflow/.env -f airflow/docker-compose.yml up -d --build
```

The first build installs psycopg into the pinned Airflow image and dbt into a separate virtual environment, avoiding incompatible Airflow/dbt dependency pins. The dbt project files are mounted at `/opt/airflow` and its profile at `/opt/airflow/dbt`. On first start, the `airflow-init` service migrates the separate metadata database. Open `http://localhost:8080`, sign in as `admin`, and unpause `weather_daily_pipeline`.

Inspect services and logs:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml ps
docker compose --env-file airflow/.env -f airflow/docker-compose.yml logs airflow-scheduler airflow-dag-processor
```

## Bài 4 — Thực hành điều phối pipeline

Mục tiêu: hiểu DAG, task, dependency, lịch chạy và retry; không cần chạy ingest/loader/dbt bằng tay nữa.

Luồng của DAG `weather_daily_pipeline`:

```text
5 task ingest độc lập → load_raw_batch → dbt_run → dbt_test
```

1. Mở `http://localhost:8080`, đăng nhập bằng user `admin`. Xem mật khẩu trong file local `airflow\runtime\logs\simple_auth_manager_passwords.json.generated`; không chia sẻ hoặc commit file này.
2. Mở DAG `weather_daily_pipeline`, bật trạng thái unpaused, rồi chọn **Trigger DAG** để tạo một manual run. Đợi các task kết thúc; retry có thể khiến run mất thêm vài phút.
3. Trong Graph/Grid, quan sát 5 task ingest không phụ thuộc nhau. `load_raw_batch` chỉ chạy khi cả 5 task thành công; `dbt_test` chỉ chạy sau `dbt_run`.
4. Mở log của một task ingest để tìm ngày và đường dẫn JSON. Mở log của loader để kiểm tra batch; mở log của hai task dbt để kiểm tra kết quả models/tests.
5. Kiểm tra dữ liệu bằng các truy vấn bên dưới. Với ngày đã xử lý, raw và mart phải có đủ 5 thành phố, mỗi thành phố một dòng.
6. Khi run thành công, chọn **Clear** cho `load_raw_batch` cùng các task downstream trong chính run đó để chạy lại cùng batch. Xác nhận khóa `(city_key, observation_date)` và cơ chế upsert không tạo dòng trùng.

Lịch chạy là **02:00 UTC mỗi ngày (09:00 giờ Việt Nam)**, `catchup=False`. DAG hiện khai báo cố định 5 city keys trong `dags/weather_daily.py`; tọa độ được lấy từ `config\cities.json`. Thêm thành phố vào config chưa tự tạo task mới: cần cập nhật danh sách `CITY_KEYS` trong DAG tương ứng.

Ngày dữ liệu được tính bằng `(data_interval_end - 1 day).date()`. Với manual run, hãy kiểm tra `data_interval_end` trong run và ngày trong log, không suy ra ngày dữ liệu chỉ từ `logical_date`. JSON được lưu theo batch ngày ở `data\raw\airflow`; loader chỉ nạp batch đó. dbt xây dựng lại mart từ nguồn raw hiện có, không chỉ riêng batch mới.

### Kiểm tra kết quả

Chạy từ thư mục gốc của repo:

```powershell
docker compose exec -T postgres psql -U weather -d weather -c "SELECT observation_date, COUNT(*) AS raw_rows FROM public.raw_weather GROUP BY observation_date ORDER BY observation_date DESC;"
docker compose exec -T postgres psql -U weather -d weather -c "SELECT observation_date, COUNT(*) AS mart_rows FROM analytics.fct_weather_daily GROUP BY observation_date ORDER BY observation_date DESC;"
docker compose exec -T postgres psql -U weather -d weather -c "SELECT city_key, observation_date, mean_temperature_c FROM analytics.fct_weather_daily WHERE observation_date = DATE '2026-10-25' ORDER BY city_key;"
```

Thay ngày ví dụ bằng ngày xuất hiện trong log của run bạn vừa trigger.

Run kiểm chứng `manual-after-pythonpath-fix` đã đạt `success`: cả 8 task thành công, raw và mart đều có 5 dòng cho ngày `2026-10-25`. Các run thất bại trước khi sửa cấu hình vẫn được giữ lại để đọc log; không cần xóa metadata.

### Đọc lỗi và retry

- `ModuleNotFoundError: No module named 'weather_pipeline'`: Compose đã đặt `PYTHONPATH=/opt/airflow` cho các dịch vụ Airflow để import package được mount. Sau khi thay cấu hình, chạy lại lệnh `up -d --build` phía trên để áp dụng.
- DAG cho phép **2 lần retry**, chờ **5 phút** giữa các lần thử. Trạng thái chờ retry chưa phải kết quả thất bại cuối cùng.
- Run kiểm chứng từng gặp `httpx.ReadTimeout` trong luồng thực thi task của Airflow rồi thành công khi retry. Nguyên nhân timeout chưa được xác định; nếu lặp lại, xem cả task log và log API server/scheduler, không mặc định đó là lỗi Open-Meteo.
- Không chạy `down -v` để chữa lỗi task: thao tác đó xóa lịch sử và metadata Airflow.

**Tiêu chí hoàn thành:** tự trigger một run thành công, giải thích được thứ tự task và ngày dữ liệu, tìm được log từng bước, và chứng minh chạy lại không tạo trùng. Sau khi bạn xác nhận hoàn thành, tài liệu ôn tập riêng sẽ được tổng hợp và push lên GitHub.

## Stop and clean up

Stop Airflow without removing its metadata volume:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml down
```

To stop and delete only the Airflow metadata database volume, explicitly run:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml down -v
```

This does not remove the existing weather database volume because the stacks use different Compose projects. The second command permanently deletes Airflow metadata and should only be used when intentionally resetting this lesson.
