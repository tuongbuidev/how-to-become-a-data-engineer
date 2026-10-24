# Weather Data Engineering Lab

Dự án học Data Engineering theo từng chặng, dùng dữ liệu thời tiết công khai từ Open-Meteo. Mỗi chặng có mục tiêu, bài thực hành và tiêu chí hoàn thành; chưa cần cài PostgreSQL, dbt hay Airflow ở tuần đầu.

## Lộ trình dự án

1. **Chặng 1 — Python + API + raw data (hiện tại):** lấy dữ liệu thời tiết và lưu nguyên trạng dưới dạng JSON.
2. **Chặng 2 — PostgreSQL:** nạp raw data vào cơ sở dữ liệu bằng thao tác có thể chạy lại.
3. **Chặng 3 — SQL + dbt:** tạo bảng phân tích và thêm kiểm thử dữ liệu.
4. **Chặng 4 — Airflow:** lập lịch, theo dõi trạng thái và chạy lại tác vụ.
5. **Chặng 5 — Spark/cloud (tùy chọn):** chỉ bổ sung sau khi pipeline cục bộ đã rõ.

## Bài học đã hoàn thành

Tài liệu ôn tập chi tiết cho các bài đã xác nhận:

- [Bài 2 — Nạp raw vào PostgreSQL](docs/lessons/02-postgresql-raw-loading.md)
- [Bài 3 — Biến đổi raw thành bảng phân tích](docs/lessons/03-sql-transformation.md)
- [Bài 3.1 — Kiểm tra chất lượng dữ liệu bằng SQL](docs/lessons/03-1-sql-data-quality.md)

Mỗi tài liệu có mục tiêu, luồng dữ liệu, giải thích khái niệm, lệnh/truy vấn mẫu và bài tập tự ôn. Các bài học tiếp theo sẽ được bổ sung vào thư mục này sau khi bạn xác nhận hoàn thành.

## Chặng 3 — SQL, mô hình dữ liệu và chất lượng dữ liệu

Tạo bảng phân tích có kiểu dữ liệu rõ ràng từ bảng raw:

```powershell
Get-Content sql\transform_daily.sql | docker compose exec -T postgres psql -U weather -d weather
```

Kiểm tra dữ liệu đã chuẩn hóa:

```sql
SELECT city_key, observation_date, mean_temperature_c,
       precipitation_mm, max_wind_speed_kmh
FROM weather_daily
ORDER BY mean_temperature_c DESC;
```

Script dùng `INSERT ... ON CONFLICT` nên có thể chạy lại an toàn. Bảng `raw_weather` giữ payload gốc; `weather_daily` phục vụ truy vấn phân tích.

### Bài 3.1 — Kiểm tra chất lượng dữ liệu bằng SQL

Sau khi chạy transform, chạy bộ kiểm tra:

```powershell
Get-Content sql\check_weather_daily.sql | docker compose exec -T postgres psql -U weather -d weather
```

Mỗi dòng là một kiểm tra độc lập. `failed_rows = 0` và `status = PASS` nghĩa là không tìm thấy bản ghi vi phạm; giá trị lớn hơn 0 và `FAIL` cần được điều tra theo quy tắc tương ứng.

Các kiểm tra xác nhận bảng có dữ liệu, mỗi bản ghi raw đã được transform, không có bản ghi phân tích mồ côi, các chỉ số thời tiết không bị thiếu, nhiệt độ nằm trong khoảng vật lý rộng từ -90 đến 60 °C và lượng mưa/tốc độ gió không âm.

**Thực hành:**

1. Chạy transform rồi chạy bộ kiểm tra hai lần; xác nhận số lượng và trạng thái không đổi.
2. Thêm một kiểm tra riêng để tìm nhiệt độ trung bình trên 35 °C.
3. Chạy kiểm tra mới; giải thích vì sao giá trị trên 35 °C là cảnh báo phục vụ điều tra chứ chưa chắc là dữ liệu sai.
4. Không xóa hoặc sửa dữ liệu raw thật chỉ để làm cho kiểm tra thành công.

Khi hoàn thành, gửi kết quả các dòng `check_name / failed_rows / status` và câu trả lời của bạn; sau khi xác nhận xong bài này, sẽ push các thay đổi và chuyển sang dbt.

## Chặng 1: chạy thử

Yêu cầu Python 3.10 trở lên; không cần cài package ngoài.

```powershell
py -3 --version
py -3 -m weather_pipeline.ingest --city hanoi --date 2026-10-21
```

Lệnh gọi API archive của Open-Meteo và lưu JSON gốc vào `data/raw/`. Ngày ví dụ có thể không được API cung cấp tùy phạm vi dữ liệu; hãy thay bằng ngày phù hợp nếu API báo lỗi. Có thể bỏ `--date` để dùng ngày hôm qua theo UTC.

## Chặng 2 — PostgreSQL

Mục tiêu: đưa các file JSON raw vào PostgreSQL, hiểu khóa chính, JSONB và cách nạp lặp an toàn.

1. Cài và khởi động Docker Desktop.
2. Cài driver PostgreSQL: `py -3 -m pip install "psycopg[binary]"`.
3. Khởi động database từ thư mục dự án: `docker compose up -d`.
4. Nạp tất cả JSON trong `data/raw`: `py -3 -m weather_pipeline.load_postgres`.
5. Mở SQL shell: `docker compose exec postgres psql -U weather -d weather`.

Compose dùng cổng host `5433` để tránh xung đột với PostgreSQL có sẵn trên máy; loader đã được cấu hình mặc định để kết nối tới cổng này. Nếu dùng công cụ bên ngoài container, kết nối tới `localhost:5433`.
6. Thử truy vấn:

```sql
SELECT city_key, observation_date,
       payload->'daily'->'temperature_2m_mean' AS mean_temp_c
FROM raw_weather
ORDER BY observation_date, city_key;

SELECT city_key, COUNT(*)
FROM raw_weather
GROUP BY city_key
ORDER BY city_key;
```

Bảng `raw_weather` dùng `(city_key, observation_date)` làm khóa chính; chạy loader lại sẽ cập nhật bản ghi thay vì tạo trùng. Mật khẩu mặc định chỉ dành cho máy cá nhân. `docker compose down -v` sẽ xóa dữ liệu database.

## Bài tập của bạn

- Đọc `weather_pipeline/ingest.py`; vẽ luồng: tham số → API → xác thực phản hồi → file.
- Chạy lệnh với hai thành phố và so sánh nội dung các file.
- Thêm một thành phố vào `config/cities.json` rồi chạy lại.
- Tự viết kiểm thử cho tên file và cấu trúc phản hồi.
- Thử ngày không hợp lệ và ghi lại cách chương trình báo lỗi.

**Đạt chặng 1 khi:** có file JSON hợp lệ, lỗi mạng/API được báo dễ hiểu, tên file không ghi đè giữa thành phố/ngày khác nhau, và kiểm thử chạy thành công.

## Kiểm thử

```powershell
py -3 -m unittest discover -s tests -v
```

## Cấu trúc

- `weather_pipeline/ingest.py`: CLI lấy và lưu raw data.
- `config/cities.json`: tọa độ các thành phố học tập.
- `data/raw/`: dữ liệu gốc; không đưa dữ liệu phát sinh vào Git.
- `tests/`: kiểm thử tự động.

## Nguồn dữ liệu

Open-Meteo Archive API: https://open-meteo.com/en/docs/historical-weather-api. Giai đoạn đầu chỉ truy cập API công khai; không lưu thông tin bí mật.
