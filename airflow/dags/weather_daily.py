import os
from datetime import datetime, timedelta
from pathlib import Path

# Thư viện Airflow chỉ có trong container, không có trong môi trường Python cục bộ.
from airflow.sdk import DAG, get_current_context  # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.bash import BashOperator  # pyright: ignore[reportMissingImports]
from psycopg.conninfo import make_conninfo

# Đường dẫn tuyệt đối tới thư mục dự án được mount trong mỗi container Airflow.
PROJECT_DIR = Path('/opt/airflow')
# Tách dữ liệu Airflow khỏi các file tạo trong những lần thực hành thủ công.
RAW_ROOT = PROJECT_DIR / 'data' / 'raw' / 'airflow'
# Khóa phải khớp với config/cities.json; mỗi thành phố có một task lấy dữ liệu.
CITY_KEYS = ['hanoi', 'ho-chi-minh-city', 'da-nang', 'hue', 'can-tho']


def ingest_city(city_key: str) -> str:
    # Ngữ cảnh thực thi cung cấp khoảng thời gian của lần chạy hiện tại.
    context = get_current_context()
    # Pipeline lấy dữ liệu archive của ngày UTC liền trước, kể cả khi chạy thủ công.
    requested_date = (context['data_interval_end'] - timedelta(days=1)).date()
    # Lưu mỗi thành phố thành một file trong thư mục mang ngày được yêu cầu.
    output_dir = RAW_ROOT / requested_date.isoformat()
    # Chỉ import khi task chạy để bước nạp DAG không cần tải các phụ thuộc ingest.
    from weather_pipeline.ingest import ingest

    # Lấy dữ liệu, lưu JSON raw và trả về đường dẫn làm kết quả task.
    path = ingest(city_key, requested_date, output_dir=output_dir)
    return str(path)


def load_batch() -> int:
    # Dùng cùng ngày dữ liệu với các task ingest để chỉ nạp batch của lần chạy này.
    context = get_current_context()
    requested_date = (context['data_interval_end'] - timedelta(days=1)).date()
    # Hoãn import loader đến lúc chạy task, không thực hiện khi Airflow khám phá DAG.
    from weather_pipeline.load_postgres import load_to_postgres

    # Tạo chuỗi kết nối có trích dẫn an toàn; mật khẩu lấy từ airflow/.env.
    database_url = make_conninfo(
        host='host.docker.internal',  # Kết nối từ container tới PostgreSQL chạy trên máy host.
        port=5433,
        dbname='weather',
        user='weather',
        password=os.environ['WEATHER_DB_PASSWORD'],
    )
    # Chỉ nạp thư mục của ngày này; loader upsert theo khóa của bảng raw.
    return load_to_postgres(RAW_ROOT / requested_date.isoformat(), database_url=database_url)


# DAG khai báo lịch và phụ thuộc giữa các task; không chạy task ngay tại đây.
with DAG(
    dag_id='weather_daily_pipeline',  # Mã định danh ổn định hiển thị trong giao diện Airflow.
    start_date=datetime(2026, 1, 1),  # Mốc bắt đầu lịch chạy, không ép tạo run cho ngày này.
    schedule='0 2 * * *',  # Chạy hằng ngày lúc 02:00 UTC (09:00 tại Việt Nam).
    catchup=False,  # Không tạo bù run cho mọi khoảng lịch đã bỏ lỡ.
    max_active_runs=1,  # Tránh để các batch theo ngày chạy chồng lấn.
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},  # Thử lại lỗi task tối đa hai lần.
    tags=['weather', 'learning'],
) as dag:
    # Mỗi PythonOperator tạo một task ingest độc lập cho một thành phố.
    ingest_tasks = [
        PythonOperator(task_id=f'ingest_{city_key.replace("-", "_")}', python_callable=ingest_city, op_kwargs={'city_key': city_key})
        for city_key in CITY_KEYS
    ]

    # Chỉ nạp các file raw sau khi mọi task ingest thành phố đã thành công.
    load = PythonOperator(task_id='load_raw_batch', python_callable=load_batch)
    # Chạy model dbt bằng môi trường ảo dbt riêng trong container.
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='/home/airflow/dbt-venv/bin/dbt run --project-dir /opt/airflow --profiles-dir /opt/airflow/dbt',
        append_env=True,  # Giữ các biến môi trường cần cho dbt và profile.
    )
    # Kiểm thử các model sau khi task dbt_run hoàn tất thành công.
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='/home/airflow/dbt-venv/bin/dbt test --project-dir /opt/airflow --profiles-dir /opt/airflow/dbt',
        append_env=True,
    )

    # Khai báo thứ tự: ingest tất cả thành phố → nạp → dựng model → kiểm thử.
    ingest_tasks >> load >> dbt_run >> dbt_test
