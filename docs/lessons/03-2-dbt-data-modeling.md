# Bài 3.2 — Mô hình hóa dữ liệu với dbt

## Mục tiêu

Sau bài này, bạn hiểu cách dùng dbt để biến bảng raw PostgreSQL thành các model phân tích có thể kiểm thử và chạy lại an toàn. Cụ thể, bạn biết:

- Khai báo project và profile dbt.
- Dùng `source()` để trỏ tới bảng nguồn bên ngoài dbt.
- Dùng `ref()` để liên kết các model và tạo dependency graph.
- Tách lớp staging và mart.
- Viết generic tests và singular SQL tests.
- Bảo vệ thông tin kết nối bằng biến môi trường.

## Bối cảnh pipeline

Pipeline hiện tại có các lớp sau:

```text
Open-Meteo API
    -> data/raw/*.json
    -> public.raw_weather
    -> analytics.stg_weather_daily (view)
    -> analytics.fct_weather_daily (table)
```

`public.raw_weather` giữ payload JSONB gốc. Model staging trích các trường cần dùng và ép kiểu. Model mart sử dụng staging để tạo bảng phân tích ổn định. Bảng cũ `public.weather_daily` vẫn được giữ nguyên để so sánh với cách transform SQL thủ công.

## 1. Cấu trúc dbt

Các file quan trọng:

| File | Vai trò |
|---|---|
| `dbt_project.yml` | Tên project, profile, đường dẫn model/test và materialization. |
| `profiles.yml.example` | Mẫu kết nối PostgreSQL, không chứa mật khẩu thật. |
| `models/sources/weather_sources.yml` | Khai báo `public.raw_weather` là source. |
| `models/staging/stg_weather_daily.sql` | View chuẩn hóa dữ liệu từ JSONB. |
| `models/marts/fct_weather_daily.sql` | Table phân tích cuối cùng. |
| `models/staging/staging.yml` | Metadata và tests cho staging. |
| `models/marts/marts.yml` | Metadata và tests cho mart. |
| `tests/assert_weather_daily_physical_ranges.sql` | Singular test cho miền giá trị vật lý. |
| `tests/assert_weather_daily_unique_city_date.sql` | Singular test cho khóa hạt dữ liệu. |

### Staging

Staging dùng:

```sql
FROM {{ source('weather', 'raw_weather') }}
```

Model này chuyển các phần tử trong JSONB thành các cột có kiểu số rõ ràng: nhiệt độ trung bình, lượng mưa và tốc độ gió lớn nhất. Staging là `view`, vì đây là lớp biến đổi nhẹ và không cần lưu thêm bản sao vật lý.

### Mart

Mart dùng:

```sql
FROM {{ ref('stg_weather_daily') }}
```

`ref()` cho dbt biết mart phụ thuộc vào staging. Mart là `table`, phù hợp cho lớp phục vụ truy vấn phân tích. Grain của mart là **một dòng cho mỗi cặp `city_key` và `observation_date`**.

## 2. Cấu hình kết nối an toàn

Cài dependency trong môi trường ảo:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Tạo profile dbt bên ngoài repository:

```powershell
New-Item -ItemType Directory -Force "$env:USERPROFILE\.dbt"
Copy-Item profiles.yml.example "$env:USERPROFILE\.dbt\profiles.yml"
$env:WEATHER_DB_PASSWORD = "weather_local_only"
```

Nếu mật khẩu PostgreSQL khác giá trị mặc định, thay giá trị biến môi trường tương ứng. Không ghi mật khẩu vào `profiles.yml.example`, source code hoặc Git.

Profile sử dụng:

- host: `localhost`
- port: `5433`
- database: `weather`
- user: `weather`
- schema đích: `analytics`

Kiểm tra profile và project:

```powershell
dbt debug --project-dir . --profiles-dir "$env:USERPROFILE\.dbt"
dbt parse --project-dir . --profiles-dir "$env:USERPROFILE\.dbt"
```

`dbt debug` kiểm tra khả năng kết nối. `dbt parse` kiểm tra cú pháp project, YAML, `source()` và `ref()` mà chưa tạo bảng.

## 3. Chạy model

```powershell
dbt run --project-dir . --profiles-dir "$env:USERPROFILE\.dbt"
```

Kết quả mong đợi:

- `analytics.stg_weather_daily` được tạo như một view.
- `analytics.fct_weather_daily` được tạo như một table.
- Số dòng mart khớp với số khóa `(city_key, observation_date)` hợp lệ trong raw.

Kiểm tra bằng PostgreSQL:

```powershell
docker compose exec postgres psql -U weather -d weather -c "\dt analytics.*"
docker compose exec postgres psql -U weather -d weather -c "SELECT COUNT(*) FROM analytics.fct_weather_daily;"
docker compose exec postgres psql -U weather -d weather -c "SELECT city_key, observation_date, mean_temperature_c FROM analytics.fct_weather_daily ORDER BY observation_date, city_key;"
```

Chạy `dbt run` lần thứ hai vẫn an toàn. dbt sẽ dựng lại view/table theo định nghĩa hiện tại; không tạo bản ghi trùng vì model mart lấy dữ liệu theo grain đã xác định.

## 4. Kiểm thử dữ liệu

Chạy toàn bộ tests:

```powershell
dbt test --project-dir . --profiles-dir "$env:USERPROFILE\.dbt"
```

Các test hiện có:

| Nhóm | Kiểm tra |
|---|---|
| Completeness | Các khóa và metric chính không được `NULL`. |
| Uniqueness/grain | Không có hai dòng cùng cặp `city_key`, `observation_date`. |
| Physical range | Nhiệt độ trong khoảng -90 đến 60 °C; lượng mưa và gió không âm. |

### Vì sao không test `city_key` là unique riêng?

Một thành phố có thể có dữ liệu của nhiều ngày, nên `city_key` một mình không phải khóa duy nhất. Khóa đúng là khóa kết hợp:

```text
(city_key, observation_date)
```

Do đó project dùng singular test `assert_weather_daily_unique_city_date.sql` để tìm các nhóm bị trùng:

```sql
SELECT city_key, observation_date
FROM {{ ref('fct_weather_daily') }}
GROUP BY city_key, observation_date
HAVING COUNT(*) > 1
```

Một singular test đạt khi truy vấn trả về không có dòng vi phạm.

## 5. Hiểu dependency graph

Liệt kê resources:

```powershell
dbt ls --project-dir . --profiles-dir "$env:USERPROFILE\.dbt"
```

Quan hệ chính:

```text
source:weather.raw_weather
        |
        v
stg_weather_daily
        |
        v
fct_weather_daily
```

- `source()` mô tả bảng tồn tại trước dbt và giúp dbt biết điểm bắt đầu của pipeline.
- `ref()` liên kết các model dbt với nhau và bảo đảm thứ tự chạy đúng.
- Khi đổi tên hoặc thay đổi model upstream, dependency graph giúp phát hiện các model downstream cần chạy lại.

## 6. Thực hành đã hoàn thành

1. Chạy `dbt run` hai lần và quan sát staging là view, mart là table.
2. Chạy `dbt test` và phân biệt test `not_null`, test grain và test physical range.
3. Thêm dữ liệu thành phố/ngày mới bằng pipeline ingest, sau đó chạy lại dbt.
4. Dùng `dbt ls` để quan sát models, source và tests.
5. Xác nhận `public.weather_daily` không bị thay đổi; mart dbt nằm trong schema `analytics`.

## 7. Các lỗi thường gặp

### Không tìm thấy profile

Đảm bảo file nằm ở `$env:USERPROFILE\.dbt\profiles.yml` và lệnh có đúng `--profiles-dir`.

### Kết nối bị từ chối

Kiểm tra Docker container và port host:

```powershell
docker compose ps
docker compose up -d
```

Từ Windows dùng `localhost:5433`. Tên service `postgres` hoặc container `weather-postgres` chỉ phù hợp trong mạng Docker.

### Biến môi trường chưa được đặt

Nếu dbt báo thiếu `WEATHER_DB_PASSWORD`, đặt lại biến trong cùng PowerShell session trước khi chạy lệnh dbt:

```powershell
$env:WEATHER_DB_PASSWORD = "weather_local_only"
```

### Test uniqueness thất bại

Không xóa dữ liệu để làm test pass. Truy vấn các nhóm trùng, kiểm tra grain và tìm nguyên nhân upstream hoặc logic model.

## Bài tập tự ôn

1. Giải thích vì sao raw nên giữ JSONB, còn mart nên có cột kiểu số rõ ràng.
2. Viết một test tìm `city_name` rỗng.
3. So sánh ưu nhược điểm của `view` và `table` trong staging/mart.
4. Thử chạy `dbt run --select stg_weather_daily`, sau đó giải thích model nào được chạy và vì sao.
5. Nêu lý do profile thật không được commit vào repository.

## Kết quả xác nhận

Bài thực hành đã được chạy thành công với:

- `dbt debug`: kết nối PostgreSQL thành công.
- `dbt parse`: project hợp lệ.
- `dbt run`: 2 model thành công.
- `dbt test`: 9/9 tests đạt.
- Python unit tests: 4/4 tests đạt.
