# Bài 3.1 — Kiểm tra chất lượng dữ liệu bằng SQL

## Mục tiêu

Sau bài này, bạn biết biến các kỳ vọng nghiệp vụ thành những kiểm tra SQL có kết quả rõ ràng. Thay vì nhìn vài dòng mẫu, mỗi kiểm tra đếm số bản ghi vi phạm và báo `PASS` hoặc `FAIL`.

## Chạy bộ kiểm tra

Trước tiên chạy phép biến đổi để bảng phân tích phản ánh dữ liệu raw hiện tại:

```powershell
Get-Content sql\transform_daily.sql | docker compose exec -T postgres psql -U weather -d weather
```

Sau đó chạy:

```powershell
Get-Content sql\check_weather_daily.sql | docker compose exec -T postgres psql -U weather -d weather
```

[check_weather_daily.sql](../../sql/check_weather_daily.sql) trả về ba cột:

- `check_name`: tên kỳ vọng được kiểm tra.
- `failed_rows`: số hàng vi phạm; bằng 0 nghĩa là không phát hiện vi phạm.
- `status`: `PASS` nếu không có hàng vi phạm, nếu không là `FAIL`.

## Các kỳ vọng hiện có

| Kiểm tra | Ý nghĩa |
|---|---|
| `weather_daily_has_rows` | Bảng đích không rỗng. |
| `all_raw_rows_are_transformed` | Mỗi khóa raw đều có hàng tương ứng trong bảng đích. |
| `all_daily_rows_have_raw_source` | Mỗi hàng phân tích có một hàng raw nguồn. |
| `daily_metrics_are_present` | Ba chỉ số thời tiết chính không bị NULL. |
| `temperature_is_within_physical_range` | Nhiệt độ nằm trong khoảng rộng -90 đến 60 °C. |
| `precipitation_is_nonnegative` | Lượng mưa không âm. |
| `wind_speed_is_nonnegative` | Tốc độ gió không âm. |

Đây là kiểm tra cấp dữ liệu. Nếu chỉ cần cảnh báo một nhóm giá trị cần xem lại, ngưỡng có thể cố ý rộng để hạn chế loại nhầm dữ liệu hợp lệ.

## Mở rộng: nhiệt độ trên 35 °C

Thêm một nhánh vào CTE `checks`, đặt trước dấu ngoặc đóng của CTE:

```sql
    UNION ALL

    SELECT
        'temperature_above_35c',
        COUNT(*)::INTEGER
    FROM weather_daily
    WHERE mean_temperature_c > 35
```

Chạy lại file SQL. Kết quả trên 0 không tự động chứng minh dữ liệu sai: một ngày nóng như vậy có thể đúng với địa điểm và thời điểm. Đây là ngưỡng cảnh báo để điều tra, khác với giới hạn vật lý rộng dùng để đánh dấu vi phạm rõ ràng.

## Cách đọc kết quả

Ví dụ một kiểm tra không phát hiện vấn đề:

```text
all_raw_rows_are_transformed | 0 | PASS
```

Ví dụ một số hàng cần điều tra:

```text
temperature_above_35c | 2 | FAIL
```

Với ngưỡng cảnh báo, có thể đặt tên trạng thái `WARN` thay vì `FAIL`, hoặc tách kiểm tra cảnh báo khỏi kiểm tra chất lượng bắt buộc. Điều quan trọng là phân biệt quy tắc dữ liệu sai chắc chắn với điều kiện đáng chú ý nhưng có thể hợp lệ.

## Bài tập ôn tập

1. Chạy transform và bộ kiểm tra hai lần; xác nhận kết quả không thay đổi.
2. Thêm kiểm tra `temperature_above_35c` như trên.
3. Ghi số dòng kết quả và giải thích vì sao ngưỡng 35 °C là cảnh báo cần điều tra, không nhất thiết là lỗi.
4. Thêm một kiểm tra hợp lý khác, chẳng hạn tên thành phố không rỗng.
5. Không sửa hoặc xóa dữ liệu thật để làm cho kiểm tra xanh; trước tiên hãy xác minh nguồn và quy tắc nghiệp vụ.

## Mở rộng kiến thức

Các kiểm tra hiện tại dùng `UNION ALL` để trả một dòng cho mỗi quy tắc. CTE đặt tên `checks` làm cho kết quả dễ đọc. Ở chặng dbt, ta sẽ chuyển một số kỳ vọng thành test của model để chúng chạy tự động trong pipeline.
