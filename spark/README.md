# Bài 5 — Xử lý dữ liệu batch với Apache Spark

Trong bài này, bạn sẽ dùng PySpark để đọc các file JSON thô do pipeline Airflow tạo ra, biến đổi dữ liệu thời tiết thành bảng có cấu trúc và ghi kết quả Parquet. Bài chạy Spark local trong Docker để làm quen với DataFrame và tư duy xử lý phân tán mà chưa cần cluster thật.

## Mục tiêu

- Hiểu vai trò của Spark và PySpark trong xử lý dữ liệu quy mô lớn.
- Đọc nhiều file JSON thành một DataFrame có schema rõ ràng.
- Dùng phép biến đổi DataFrame để trích các chỉ số thời tiết.
- Ghi kết quả sang Parquet và kiểm tra khả năng đọc lại.

## Dữ liệu đầu vào

Airflow lưu raw JSON theo cấu trúc `data/raw/airflow/<ngày>/<ngày>_<city_key>.json`. Hãy chạy thành công Airflow ít nhất một lần trước bài này, hoặc chọn một thư mục ngày đã có dữ liệu.

Mỗi file có metadata (`city_key`, `city_name`, `requested_date`, `source`, `fetched_at`) và payload Open-Meteo bên trong. Spark cần lấy các trường cần phân tích từ cả hai phần.

## Bước 1 — Kiểm tra dữ liệu đầu vào

Từ thư mục gốc repo, liệt kê các batch đã có:

```powershell
Get-ChildItem data\raw\airflow -Directory
Get-ChildItem data\raw\airflow\2026-10-25 -File
```

Thay ngày ví dụ bằng thư mục thực tế trên máy bạn. Mở một JSON và xác định đường dẫn tới `daily.time`, `daily.temperature_2m_mean`, `daily.precipitation_sum` và `daily.wind_speed_10m_max`.

## Bước 2 — Thêm Spark vào môi trường thực hành

Chúng ta sẽ trước tiên đọc dữ liệu bằng PySpark chạy local qua container chính thức `spark`. Bước triển khai Compose/runner và mã ETL sẽ được hoàn thiện sau khi xác nhận đường dẫn đầu vào tồn tại và đã hiểu cấu trúc payload.

Không cài PySpark vào môi trường Airflow: scheduler và Spark có mục đích, dependency khác nhau. Pipeline Airflow hiện tại vẫn chạy ingest, PostgreSQL và dbt như trước.

## Thực hành

1. Xác nhận có ít nhất một batch JSON do Airflow tạo.
2. Chọn một ngày batch, đếm số file và xác nhận có một file cho mỗi thành phố.
3. Phác thảo schema đầu ra: `city_key`, `city_name`, `observation_date`, `mean_temperature_c`, `precipitation_mm`, `max_wind_speed_kmh`.
4. Dự đoán số bản ghi đầu ra và kiểu dữ liệu mong muốn cho từng cột.

Sau khi bạn hoàn thành bước chuẩn bị này, chúng ta sẽ tạo Spark job, chạy và kiểm tra bảng Parquet đầu ra. Chưa cần push bài 5; theo quy ước, tài liệu ôn tập và push sẽ thực hiện khi bạn xác nhận hoàn thành bài.