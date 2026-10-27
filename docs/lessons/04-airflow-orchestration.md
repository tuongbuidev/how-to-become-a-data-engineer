# Bài 4 — Tự động hóa pipeline với Airflow

## Mục tiêu

- Hiểu DAG, task, dependency, lịch chạy và retry trong Apache Airflow.
- Điều phối pipeline từ bước lấy dữ liệu đến nạp PostgreSQL và chạy dbt.
- Theo dõi trạng thái, đọc log và chạy lại task an toàn.

## Kiến trúc thực hành

Airflow chạy trong Docker Compose riêng, với PostgreSQL metadata riêng; dữ liệu thời tiết vẫn được nạp vào PostgreSQL hiện có ở cổng host `5433`. Airflow UI được bind ở `127.0.0.1:8080`.

Luồng DAG `weather_daily_pipeline`:

```text
5 task ingest độc lập → load_raw_batch → dbt_run → dbt_test
```

Năm task ingest có thể chạy song song. Chỉ sau khi cả năm thành công, Airflow thực thi `load_raw_batch`, tiếp theo `dbt_run` và `dbt_test`.

## Cấu hình và khởi động

1. Khởi động Docker Desktop và PostgreSQL thời tiết:

```powershell
docker compose up -d postgres
```

2. Tạo cấu hình bí mật cục bộ, điền các giá trị thực, không commit `airflow/.env`:

```powershell
Copy-Item airflow\.env.example airflow\.env
notepad airflow\.env
```

`WEATHER_DB_PASSWORD` là mật khẩu PostgreSQL role `weather`. Các giá trị `AIRFLOW_DB_PASSWORD`, `AIRFLOW_SECRET_KEY` và `AIRFLOW_JWT_SECRET` nên là giá trị ngẫu nhiên riêng biệt.

3. Xác thực Compose config rồi build và chạy dịch vụ:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml config --quiet
docker compose --env-file airflow/.env -f airflow/docker-compose.yml up -d --build
docker compose --env-file airflow/.env -f airflow/docker-compose.yml ps
```

4. Mở `http://localhost:8080`, đăng nhập bằng user `admin`. Lần khởi động đầu tiên, mật khẩu tự sinh nằm trong `airflow\runtime\logs\simple_auth_manager_passwords.json.generated`. Giữ bí mật file này. Mở DAG `weather_daily_pipeline` và unpause DAG nếu đang bị pause.

## Quan sát và kiểm tra

1. Chọn **Trigger DAG** trong giao diện Airflow để tạo manual run.
2. Mở Graph/Grid; quan sát năm task ingest độc lập và các dependency phía sau.
3. Mở task log để tìm ngày dữ liệu, tên batch, đường dẫn file, số dòng đã nạp và kết quả dbt.
4. Run kiểm chứng `manual-after-pythonpath-fix` hoàn thành thành công ở cả 8 task. PostgreSQL có 5 dòng raw và 5 dòng mart cho ngày `2026-10-25`.
5. Truy vấn dữ liệu cho ngày xuất hiện trong log của run:

```powershell
docker compose exec -T postgres psql -U weather -d weather -c "SELECT observation_date, COUNT(*) AS raw_rows FROM public.raw_weather GROUP BY observation_date ORDER BY observation_date DESC;"
docker compose exec -T postgres psql -U weather -d weather -c "SELECT observation_date, COUNT(*) AS mart_rows FROM analytics.fct_weather_daily GROUP BY observation_date ORDER BY observation_date DESC;"
docker compose exec -T postgres psql -U weather -d weather -c "SELECT city_key, observation_date, mean_temperature_c FROM analytics.fct_weather_daily WHERE observation_date = DATE '2026-10-25' ORDER BY city_key;"
```

Thay ngày ví dụ bằng ngày của batch bạn vừa chạy. Mỗi batch hiện có năm thành phố, vì danh sách task được khai báo tĩnh trong DAG.

## Khái niệm cần nắm

- **DAG:** đồ thị có hướng không chu trình mô tả luồng công việc.
- **Task/operator:** một công việc trong DAG. `PythonOperator` gọi hàm Python; `BashOperator` chạy lệnh dbt.
- **Dependency:** biểu thức `ingest_tasks >> load >> dbt_run >> dbt_test` đảm bảo tất cả ingest hoàn tất trước khi nạp; dbt test chạy sau dbt run.
- **Schedule:** biểu thức cron `0 2 * * *` chạy hằng ngày lúc 02:00 UTC (09:00 giờ Việt Nam).
- **Retry:** `retries=2` cho phép thử lại task lỗi tối đa hai lần, với khoảng nghỉ `retry_delay=5 phút`.
- **Catchup:** `catchup=False` ngăn Airflow tự tạo toàn bộ các run lịch cũ khi bật DAG.
- **Idempotency:** loader dùng khóa `(city_key, observation_date)` và `ON CONFLICT ... DO UPDATE`, vì thế nạp lại cùng batch cập nhật bản ghi thay vì nhân đôi.

### Ngày dữ liệu và logical date

Trong hàm ingest và loader, ngày dữ liệu được tính bằng:

```python
requested_date = (context['data_interval_end'] - timedelta(days=1)).date()
```

Airflow cung cấp `data_interval_end` theo khoảng dữ liệu của run. Với manual run, không nên giả định `logical_date` là ngày được ingest; hãy xem interval và task log. Dữ liệu được ghi vào `data/raw/airflow/<ngày>/`.

## Lỗi thường gặp

- **`ModuleNotFoundError: No module named 'weather_pipeline'`:** package được mount vào `/opt/airflow`; Compose khai báo `PYTHONPATH=/opt/airflow` để scheduler và worker tìm được package.
- **`httpx.ReadTimeout`:** một lần kiểm chứng đã gặp timeout tạm thời trong luồng thực thi Airflow và thành công khi retry. Nếu lặp lại, xem task log cùng log API server/scheduler; nguyên nhân khi đó chưa được xác định.
- **Lỗi xác thực PostgreSQL:** kiểm tra `WEATHER_DB_PASSWORD` trong `airflow/.env` có khớp mật khẩu thực của PostgreSQL role `weather` hay không.
- Không dùng `docker compose down -v` để chữa lỗi task; tùy stack, thao tác này xóa volume chứa metadata và lịch sử Airflow.

## Bài tập tự ôn

1. Giải thích vì sao năm task ingest có thể chạy đồng thời nhưng task nạp phải đợi tất cả.
2. Giải thích sự khác biệt giữa `logical_date` và ngày dữ liệu lấy từ `data_interval_end`.
3. Clear task `load_raw_batch` cùng các task downstream trong cùng run để nạp lại batch, rồi xác nhận số dòng theo ngày không tăng.
4. Tìm một task thất bại cũ, đọc log để xác định lỗi và phân biệt lỗi gốc với trạng thái sau retry.
5. Thêm thành phố: cập nhật `config/cities.json` và danh sách `CITY_KEYS` trong DAG; giải thích vì sao chỉ cập nhật config chưa tạo task Airflow mới.

## Hoàn thành bài

Bài thực hành được xác nhận hoàn thành khi bạn có thể tự trigger một run thành công, giải thích thứ tự task và ngày dữ liệu, tìm log của từng bước, và chứng minh chạy lại cùng batch không tạo dòng trùng.

## Dừng dịch vụ

Dừng nhưng giữ metadata:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml down
```

Chỉ xóa metadata Airflow khi cố ý reset bài học:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml down -v
```
