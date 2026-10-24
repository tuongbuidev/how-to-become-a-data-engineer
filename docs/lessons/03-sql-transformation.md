# Bài 3 — Biến đổi raw thành bảng phân tích

## Mục tiêu

Sau bài này, bạn hiểu vì sao pipeline tách vùng raw khỏi bảng phân tích, cách trích trường JSONB và chuyển sang kiểu dữ liệu SQL, cũng như cách giữ phép biến đổi chạy lại được.

## Từ raw đến bảng phân tích

```text
raw_weather.payload (JSONB)
    -> sql/transform_daily.sql
    -> weather_daily (các cột có kiểu dữ liệu rõ ràng)
```

`raw_weather` là lớp lưu giữ dữ liệu nhận được từ nguồn. `weather_daily` là mô hình dạng bảng thuận tiện cho phân tích: mỗi dòng đại diện một thành phố trong một ngày, các chỉ số là cột kiểu số.

## Chạy phép biến đổi

Đảm bảo PostgreSQL đang chạy và đã nạp raw data, sau đó chạy:

```powershell
Get-Content sql\transform_daily.sql | docker compose exec -T postgres psql -U weather -d weather
```

Script [transform_daily.sql](../../sql/transform_daily.sql) tạo `weather_daily` nếu bảng chưa tồn tại, sau đó chọn dữ liệu từ `raw_weather` và ghi vào bảng đích.

Các trường được trích từ JSONB như sau:

```sql
payload->'daily'->'temperature_2m_mean'->>0
```

Phép `->` đi sâu qua các object JSON; `->>0` lấy phần tử đầu của mảng dưới dạng text. Cast như `::NUMERIC(5, 2)` chuyển text thành số để có thể tính toán và sắp xếp đúng kiểu dữ liệu.

## Grain và khóa

Grain của `weather_daily` là một dòng cho mỗi cặp thành phố/ngày. Khóa chính `(city_key, observation_date)` thể hiện grain đó và ngăn tạo hai bản ghi phân tích cho cùng một quan sát.

Các cột gồm:

- `city_key`, `city_name`: định danh và tên thành phố.
- `observation_date`: ngày quan sát.
- `mean_temperature_c`, `precipitation_mm`, `max_wind_speed_kmh`: chỉ số đã chuyển thành số.
- `source`, `fetched_at`, `transformed_at`: nguồn và thời điểm xử lý.

## Tính chạy lại an toàn

Cuối câu lệnh là `ON CONFLICT (city_key, observation_date) DO UPDATE`. Khi chạy lại, hàng đã có được cập nhật từ raw mới nhất. Đây là upsert, tương tự chiến lược nạp raw: dữ liệu được đồng bộ theo khóa thay vì thêm bản sao.

## Truy vấn kiểm tra và phân tích

```sql
SELECT city_key, observation_date,
       mean_temperature_c, precipitation_mm, max_wind_speed_kmh
FROM weather_daily
ORDER BY mean_temperature_c DESC;
```

Tính nhiệt độ trung bình theo thành phố:

```sql
SELECT city_key, ROUND(AVG(mean_temperature_c), 2) AS avg_temperature_c
FROM weather_daily
GROUP BY city_key
ORDER BY avg_temperature_c DESC;
```

Đối chiếu số lượng raw và bảng phân tích:

```sql
SELECT
    (SELECT COUNT(*) FROM raw_weather) AS raw_rows,
    (SELECT COUNT(*) FROM weather_daily) AS daily_rows;
```

Trong phạm vi hiện tại, mỗi hàng raw tương ứng một hàng daily. Khi có lọc, dữ liệu thiếu hoặc nhiều nguồn, hai số lượng có thể khác nhau và cần giải thích theo quy tắc nghiệp vụ.

## Bài tập tự ôn

1. Chạy transform hai lần; xác nhận `weather_daily` không bị nhân đôi.
2. Thêm `city_name` vào truy vấn kết quả.
3. Tính lượng mưa trung bình theo thành phố.
4. Giải thích grain của bảng và lý do khóa chính gồm cả `city_key` lẫn `observation_date`.
5. Trong DBeaver, thử kết nối từ Windows bằng `localhost:5433`; không dùng hostname container `weather-postgres` từ máy host.
