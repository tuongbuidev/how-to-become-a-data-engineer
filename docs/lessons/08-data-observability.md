# Bài 8 — Data Observability bằng SQL

## Mục tiêu

- Đo độ mới, số lượng và độ phủ dữ liệu trong mart.
- Phân biệt thời điểm pipeline lấy dữ liệu với ngày dữ liệu được quan sát.
- Nhận diện khi số liệu tổng thể che giấu dữ liệu thiếu ở một thành phố.
- Chuẩn bị các chỉ số để lưu snapshot theo từng lần chạy pipeline.

## Data Quality và Data Observability

- **Data Quality** kiểm tra dữ liệu có thỏa quy tắc hay không, ví dụ `city_name` không rỗng và khóa thành phố-ngày không bị trùng.
- **Data Observability** theo dõi sức khỏe dữ liệu theo thời gian: dữ liệu có mới không, số dòng có thay đổi bất thường không, và các thành phố có được cập nhật đủ không.

Một DAG run thành công không tự nó chứng minh mart mới hoặc đủ dữ liệu. Ngược lại, một chỉ số lệch cũng cần được điều tra theo lịch chạy và lịch sử thêm thành phố trước khi kết luận là lỗi.

## Thực hành SQL

Chạy các truy vấn từ PowerShell tại thư mục dự án. PostgreSQL dự án được công bố ở cổng host `5433`:

```powershell
docker compose exec postgres psql -U weather -d weather
```

### 1. Đo tổng quan và độ mới

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT city_key) AS city_count,
    MAX(observation_date) AS latest_observation_date,
    CURRENT_DATE - MAX(observation_date) AS observation_lag_days,
    MAX(fetched_at) AS latest_fetch_at
FROM analytics.fct_weather_daily;
```

Kết quả đã thực hành: **58 dòng**, **5 thành phố**, ngày quan sát mới nhất **2026-11-09**, độ trễ **1 ngày**, và `latest_fetch_at` là **2026-11-10 02:05:34 UTC**.

Pipeline lấy dữ liệu của ngày hôm trước nên độ trễ 1 ngày có thể là bình thường. `fetched_at` biểu thị thời điểm lấy dữ liệu, không phải ngày quan sát: chạy lại một ngày lịch sử có thể làm `fetched_at` mới hơn mà không làm `observation_date` mới hơn.

### 2. Đo từng thành phố

```sql
SELECT
    city_key,
    COUNT(*) AS row_count,
    MAX(observation_date) AS latest_observation_date,
    CURRENT_DATE - MAX(observation_date) AS lag_days
FROM analytics.fct_weather_daily
GROUP BY city_key
ORDER BY city_key;
```

Kết quả đã thực hành: cả 5 thành phố đều có dữ liệu mới nhất đến **2026-11-09**. Cần Thơ và TP.HCM có 11 dòng; Hà Nội, Đà Nẵng và Huế có 12 dòng. Chênh lệch số dòng chưa đủ kết luận là lỗi, vì các thành phố có thể được thêm vào ở thời điểm khác nhau hoặc chưa được backfill đủ lịch sử.

### 3. Đo độ phủ thành phố theo ngày

```sql
SELECT
    observation_date,
    COUNT(*) AS row_count,
    COUNT(DISTINCT city_key) AS city_count,
    CASE
        WHEN COUNT(DISTINCT city_key) = 5 THEN 'OK'
        ELSE 'CHECK'
    END AS coverage_status
FROM analytics.fct_weather_daily
GROUP BY observation_date
ORDER BY observation_date DESC
LIMIT 14;
```

Số `5` là số thành phố kỳ vọng trong lần thực hành này, không phải quy tắc cố định cho mọi môi trường. Tổng thể có đủ 5 thành phố không đảm bảo rằng mỗi ngày đều đủ cả 5. Cần đối chiếu ngày `CHECK` với lịch sử thêm thành phố, backfill và DAG runs.

Thoát khỏi `psql` bằng `\q`.

## Kết quả và bài tập

Người học đã chạy cả ba truy vấn. Số liệu vừa ghi lại phản ánh trạng thái database tại thời điểm thực hành; số dòng và thời điểm fetch sẽ thay đổi khi pipeline chạy hoặc dữ liệu được backfill.

1. Tìm ngày có `coverage_status = 'CHECK'` và giải thích nguyên nhân có thể có.
2. Giải thích vì sao `MAX(observation_date)` toàn mart có thể che giấu một thành phố bị chậm.
3. So sánh `observation_lag_days` với `fetched_at`; nêu tình huống một chỉ số tốt lên nhưng chỉ số kia không đổi.
4. Không xem số dòng khác nhau giữa các thành phố là lỗi nếu chưa xác minh thời điểm bắt đầu thu thập và chính sách backfill.

## Tự động lưu snapshot theo DAG run

DAG `weather_daily_pipeline` có task `record_observability_snapshot` chạy sau `dbt_test`. Vì task này phụ thuộc vào `dbt_test`, snapshot chỉ được ghi khi toàn bộ dbt tests thành công; nếu test thất bại, pipeline không ghi trạng thái thành công giả.

Task tự tạo schema `observability` và bảng `weather_pipeline_snapshots` nếu chưa tồn tại. Mỗi snapshot được khóa bằng `(dag_id, run_id)`, nên retry cùng một DAG run cập nhật bản ghi thay vì tạo bản sao. Bản ghi gồm:

- `recorded_at`: thời điểm ghi snapshot.
- `row_count`, `city_count`, `latest_observation_date`, `observation_lag_days`, `latest_fetch_at`: chỉ số tổng quan của mart.
- `dbt_test_status`: `passed`, vì task chỉ chạy sau `dbt_test` thành công.
- `city_metrics`: số dòng và ngày quan sát mới nhất của từng thành phố dưới dạng JSONB.

Sau khi DAG run hoàn thành, xem các snapshot gần nhất:

```powershell
docker compose exec postgres psql -U weather -d weather -c "SELECT dag_id, run_id, recorded_at, row_count, city_count, latest_observation_date, observation_lag_days, latest_fetch_at, dbt_test_status, city_metrics FROM observability.weather_pipeline_snapshots ORDER BY recorded_at DESC LIMIT 10;"
```

So sánh qua thời gian để phát hiện xu hướng. Số dòng giảm có thể do xóa dữ liệu hoặc thay đổi phạm vi; lag tăng có thể báo dữ liệu cũ nhưng cũng cần đối chiếu lịch batch; độ phủ thành phố thay đổi cần so với cấu hình và thời điểm thêm thành phố. Snapshot là tín hiệu điều tra, không tự động kết luận nguyên nhân.

## Bài thực hành tiếp theo

1. Chạy DAG `weather_daily_pipeline` từ Airflow UI hoặc API và chờ `record_observability_snapshot` thành công.
2. Chạy truy vấn snapshot phía trên, xác nhận `dbt_test_status = 'passed'` và đối chiếu các chỉ số với mart.
3. Trigger lại DAG để tạo run thứ hai; so sánh `row_count`, độ mới và `city_metrics` giữa hai run.
4. Giải thích vì sao snapshot không được tạo nếu `dbt_test` thất bại, và vì sao `dbt_test_status` không thể ghi nhận trạng thái `failed` theo thiết kế hiện tại.

## Kết quả đã xác minh

Đã trigger hai DAG runs thủ công liên tiếp. Cả hai run đều thành công; run đầu có một số task ingest retry do timeout mạng tạm thời nhưng cuối cùng hoàn tất. Hai snapshot đều ghi nhận:

- `row_count = 58`
- `city_count = 5`
- `latest_observation_date = 2026-11-09`
- `observation_lag_days = 1`
- `dbt_test_status = 'passed'`

Các chỉ số giống nhau cho thấy lần chạy thứ hai không tạo thêm ngày hoặc thành phố mới; thao tác upsert vẫn giữ dữ liệu idempotent. Đây là baseline để các lần chạy sau phát hiện biến động bất thường.

## Bước tiếp theo — cảnh báo không chặn pipeline

Snapshot hiện có thêm hai trường:

- `observability_status`: `healthy` hoặc `warning`.
- `warning_reasons`: danh sách nguyên nhân dạng JSONB.

Task đánh dấu `warning` khi không có dòng, số thành phố thấp hơn số thành phố trong `CITY_KEYS`, ngày quan sát trễ hơn một ngày, hoặc số dòng giảm so với snapshot trước. Đây là **cảnh báo có thể hợp lệ**, không tự động làm DAG thất bại; người vận hành cần xem nguyên nhân trước khi quyết định xử lý.

Xem các trạng thái gần nhất:

```powershell
docker compose exec postgres psql -U weather -d weather -c "SELECT run_id, recorded_at, observability_status, warning_reasons, row_count, city_count, latest_observation_date FROM observability.weather_pipeline_snapshots ORDER BY recorded_at DESC LIMIT 10;"
```

Thực hành: sau khi trigger một run mới, xác nhận trạng thái hiện tại là `healthy` và `warning_reasons` là `[]`. Sau đó giải thích tình huống nào có thể tạo `warning` hợp lệ, chẳng hạn thêm thành phố mới vào cấu hình nhưng chưa backfill dữ liệu lịch sử.

Lần kiểm chứng đầu tiên phát hiện task snapshot lỗi vì truy vấn SQL có 5 placeholder nhưng chỉ nhận 4 tham số. Đã bổ sung tham số `dag_id` còn thiếu trong `previous_metrics`. Ingest, Spark, `dbt_run` và `dbt_test` của run đó đều thành công; lần retry sau khi sửa đã hoàn tất thành công.

### Kết quả xác minh cảnh báo end-to-end

Sau khi sửa tham số, DAG run `manual__2026-11-10T05:13:13.674423+00:00` hoàn tất thành công, bao gồm `dbt_test` và `record_observability_snapshot`. Snapshot được ghi nhận với:

- `row_count = 58`, `city_count = 5`
- `latest_observation_date = 2026-11-09`, `observation_lag_days = 1`
- `dbt_test_status = 'passed'`
- `observability_status = 'healthy'`, `warning_reasons = []`

Điều này xác nhận cảnh báo được tính và lưu mà không làm DAG thất bại khi dữ liệu đáp ứng ngưỡng hiện tại.
