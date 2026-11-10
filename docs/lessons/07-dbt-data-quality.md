# Bài 7 — Data Quality với dbt

## Mục tiêu

- Phân biệt kiểm tra dữ liệu với kiểm tra task orchestration.
- Khai báo kỳ vọng ở mức cột bằng built-in tests của dbt.
- Viết và đọc singular tests cho quy tắc nghiệp vụ.
- Xem cách test thất bại làm DAG dừng ở `dbt_test`.

## Mô hình và grain

Mart `fct_weather_daily` có grain một dòng cho mỗi thành phố và ngày quan sát. Vì vậy khóa logic là cặp `(city_key, observation_date)`. `unique` trên `observation_date` riêng không đúng, vì nhiều thành phố có dữ liệu cùng ngày; do đó tính duy nhất của cặp khóa được kiểm tra bằng singular test.

## Quy tắc chất lượng

Trong `models/marts/marts.yml`:

- `not_null`: kiểm tra các định danh và chỉ số thiết yếu.

Trong `tests/` có các singular tests:

- `assert_weather_daily_city_name_not_blank.sql`: trả về dòng có `city_name` là `NULL`, rỗng hoặc chỉ chứa khoảng trắng. Đây là lỗi dữ liệu chắc chắn vì không thể nhận diện thành phố.
- `assert_weather_daily_unique_city_date.sql`: truy vấn các cặp khóa thành phố-ngày bị lặp.
- `assert_weather_daily_physical_ranges.sql`: trả về các dòng có nhiệt độ ngoài khoảng -90..60 °C, lượng mưa âm hoặc tốc độ gió âm.

Các test SQL đạt khi không trả về dòng nào. Built-in `unique` và `not_null` cũng là các test sinh truy vấn kiểm tra dữ liệu. Ngưỡng lượng mưa cao không tự động là lỗi: mưa lớn có thể là hiện tượng thật. Hãy xác nhận đơn vị và quy tắc nghiệp vụ; nếu chỉ cần theo dõi, hãy dùng cảnh báo thay vì chặn pipeline.

## Thực hành

### 1. Chạy dbt tests

Khi Airflow stack đang chạy và PostgreSQL dự án đã sẵn sàng:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml exec -T airflow-scheduler /home/airflow/dbt-venv/bin/dbt test --project-dir /opt/airflow --profiles-dir /opt/airflow/dbt
```

Chạy riêng test tên thành phố:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml exec -T airflow-scheduler /home/airflow/dbt-venv/bin/dbt test --project-dir /opt/airflow --profiles-dir /opt/airflow/dbt --select assert_weather_daily_city_name_not_blank
```

Airflow phải mount thư mục `../tests` vào `/opt/airflow/tests` trong container; cấu hình này nằm trong `airflow/docker-compose.yml`. Nếu dbt báo selector không khớp hoặc `Nothing to do`, kiểm tra mount và liệt kê các test được nhận diện:

```powershell
docker compose --env-file airflow/.env -f airflow/docker-compose.yml exec -T airflow-scheduler /home/airflow/dbt-venv/bin/dbt ls --project-dir /opt/airflow --profiles-dir /opt/airflow/dbt --resource-type test
```

Hoặc chạy toàn bộ luồng qua Airflow UI: trigger `weather_daily_pipeline` và mở task `dbt_test` để xem log. `dbt_run` tạo/cập nhật model trước; `dbt_test` kiểm tra dữ liệu sau đó.

### 2. Đọc kết quả

- `PASS`: không có dòng vi phạm quy tắc.
- `FAIL`: truy vấn test trả về một hoặc nhiều dòng vi phạm.
- Với singular test, dbt log tên test, số lỗi và có thể ghi kết quả chi tiết trong thư mục `target/` tùy cấu hình.

Không nên sửa dữ liệu thật chỉ để làm test xanh. Trước tiên hãy xem các dòng vi phạm và xác định lỗi ở nguồn, logic biến đổi hay giả định nghiệp vụ.

### 3. Thử lỗi an toàn

Không chèn dữ liệu giả vào database dùng chung. Có thể thử trong database/schema dùng riêng hoặc tạm thời sửa điều kiện trong một singular test để test cố ý trả ra dòng vi phạm, chạy test xác nhận trạng thái thất bại, rồi hoàn nguyên thay đổi. Sau đó chạy lại suite để xác nhận trạng thái xanh.

## Kết nối với Airflow

DAG đã có chuỗi `load_raw_batch → dbt_run → dbt_test`. Lỗi test làm task `dbt_test` và DAG run thất bại, giúp chất lượng dữ liệu trở thành cổng kiểm soát thay vì chỉ là báo cáo.

## Bài tập

1. Chạy test `assert_weather_daily_city_name_not_blank` riêng, rồi ghi lại kết quả.
2. Giải thích vì sao grain thành phố-ngày cần kiểm tra khóa kép.
3. Đề xuất một ngưỡng lượng mưa để cảnh báo. Nêu rõ đơn vị, lý do chọn ngưỡng và vì sao vượt ngưỡng chưa chắc là dữ liệu sai.
4. Thử một test thất bại trong môi trường cô lập, xem log Airflow/dbt, rồi khôi phục và chạy xanh. Không chèn dữ liệu thử vào database dùng chung.
5. Kiểm tra DAG không đánh dấu thành công khi `dbt_test` thất bại.

## Kết quả đã xác minh

Test `assert_weather_daily_city_name_not_blank` được dbt 1.12.6 nhận diện và chạy riêng thành công (`PASS=1`). Lệnh liệt kê test nhận diện tổng cộng 14 data tests: 11 schema tests và 3 singular SQL tests. Lần chạy toàn suite trước đó, trước khi singular tests được mount vào container, chỉ chạy 11 schema tests; vì vậy cần chạy lại toàn suite sau khi thay đổi môi trường để xác nhận tất cả 14 test.
