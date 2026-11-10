import os
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Thư viện Airflow chỉ có trong container, không có trong môi trường Python cục bộ.
from airflow.sdk import DAG, get_current_context  # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from airflow.providers.standard.operators.bash import BashOperator  # pyright: ignore[reportMissingImports]
from psycopg import connect
from psycopg.conninfo import make_conninfo

# Đường dẫn tuyệt đối tới thư mục dự án được mount trong mỗi container Airflow.
PROJECT_DIR = Path('/opt/airflow')
# Tách dữ liệu Airflow khỏi các file tạo trong những lần thực hành thủ công.
RAW_ROOT = PROJECT_DIR / 'data' / 'raw' / 'airflow'
# Khóa phải khớp với config/cities.json; mỗi thành phố có một task lấy dữ liệu.
CITY_KEYS = ['hanoi', 'ho-chi-minh-city', 'da-nang', 'hue', 'can-tho']


def get_requested_date() -> date:
    # Lần chạy thủ công có thể không có data interval; khi đó dùng ngày UTC hiện tại.
    context = get_current_context()
    interval_end = context.get('data_interval_end') or datetime.now(timezone.utc)
    # Lấy ngày liền trước theo quy ước batch của pipeline.
    return (interval_end - timedelta(days=1)).date()


def ingest_city(city_key: str) -> str:
    requested_date = get_requested_date()
    # Lưu mỗi thành phố thành một file trong thư mục mang ngày được yêu cầu.
    output_dir = RAW_ROOT / requested_date.isoformat()
    # Chỉ import khi task chạy để bước nạp DAG không cần tải các phụ thuộc ingest.
    from weather_pipeline.ingest import ingest

    # Lấy dữ liệu, lưu JSON raw và trả về đường dẫn làm kết quả task.
    path = ingest(city_key, requested_date, output_dir=output_dir)
    return str(path)


def run_spark_batch() -> str:
    """Chạy Spark local để trải raw JSON thành Parquet theo ngày của DAG."""
    requested_date = get_requested_date()
    input_path = RAW_ROOT / requested_date.isoformat() / '*.json'
    output_path = PROJECT_DIR / 'data' / 'processed' / 'spark' / requested_date.isoformat()

    # Dùng cùng image Airflow nhưng venv riêng để Spark không ảnh hưởng dependency Airflow.
    env = os.environ.copy()
    env['PYSPARK_PYTHON'] = '/home/airflow/spark-venv/bin/python'
    command = [
        '/home/airflow/spark-venv/bin/spark-submit',
        '--master',
        'local[*]',
        '/opt/airflow/spark/jobs/weather_batch.py',
        '--input',
        str(input_path),
        '--output',
        str(output_path),
    ]
    subprocess.run(command, check=True, env=env)
    return str(output_path)


def record_observability_snapshot() -> None:
    """Lưu chỉ số mart và trạng thái dbt test cho DAG run hiện tại."""
    context = get_current_context()
    dag_run = context['dag_run']
    database_url = make_conninfo(
        host='host.docker.internal',
        port=5433,
        dbname='weather',
        user='weather',
        password=os.environ['WEATHER_DB_PASSWORD'],
    )

    with connect(database_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE SCHEMA IF NOT EXISTS observability;
            CREATE TABLE IF NOT EXISTS observability.weather_pipeline_snapshots (
                dag_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                row_count BIGINT NOT NULL,
                city_count INTEGER NOT NULL,
                latest_observation_date DATE,
                observation_lag_days INTEGER,
                latest_fetch_at TIMESTAMPTZ,
                dbt_test_status TEXT NOT NULL,
                observability_status TEXT NOT NULL,
                warning_reasons JSONB NOT NULL,
                city_metrics JSONB NOT NULL,
                PRIMARY KEY (dag_id, run_id)
            );
            ALTER TABLE observability.weather_pipeline_snapshots
                ADD COLUMN IF NOT EXISTS observability_status TEXT NOT NULL DEFAULT 'healthy';
            ALTER TABLE observability.weather_pipeline_snapshots
                ADD COLUMN IF NOT EXISTS warning_reasons JSONB NOT NULL DEFAULT '[]'::jsonb;
            """
        )
        cursor.execute(
            """
            WITH city_metrics AS (
                SELECT
                    city_key,
                    COUNT(*) AS row_count,
                    MAX(observation_date) AS latest_observation_date
                FROM analytics.fct_weather_daily
                GROUP BY city_key
            ), aggregate_metrics AS (
                SELECT
                    COUNT(*) AS row_count,
                    COUNT(DISTINCT city_key) AS city_count,
                    MAX(observation_date) AS latest_observation_date,
                    CURRENT_DATE - MAX(observation_date) AS observation_lag_days,
                    MAX(fetched_at) AS latest_fetch_at
                FROM analytics.fct_weather_daily
            ), previous_metrics AS (
                SELECT row_count
                FROM observability.weather_pipeline_snapshots
                WHERE dag_id = %s
                ORDER BY recorded_at DESC
                LIMIT 1
            )
            INSERT INTO observability.weather_pipeline_snapshots (
                dag_id,
                run_id,
                row_count,
                city_count,
                latest_observation_date,
                observation_lag_days,
                latest_fetch_at,
                dbt_test_status,
                observability_status,
                warning_reasons,
                city_metrics
            )
            SELECT
                %s,
                %s,
                metrics.row_count,
                metrics.city_count,
                metrics.latest_observation_date,
                metrics.observation_lag_days,
                metrics.latest_fetch_at,
                'passed',
                CASE
                    WHEN metrics.row_count = 0
                        OR metrics.city_count < %s
                        OR metrics.observation_lag_days > 1
                        OR (previous.row_count IS NOT NULL AND metrics.row_count < previous.row_count)
                    THEN 'warning'
                    ELSE 'healthy'
                END,
                array_to_json(array_remove(ARRAY[
                    CASE WHEN metrics.row_count = 0 THEN 'no_rows' END,
                    CASE WHEN metrics.city_count < %s THEN 'missing_expected_city' END,
                    CASE WHEN metrics.observation_lag_days > 1 THEN 'stale_observation_date' END,
                    CASE WHEN previous.row_count IS NOT NULL AND metrics.row_count < previous.row_count THEN 'row_count_decreased' END
                ], NULL))::jsonb,
                COALESCE(
                    (SELECT jsonb_agg(to_jsonb(city_metrics) ORDER BY city_key) FROM city_metrics),
                    '[]'::jsonb
                )
            FROM aggregate_metrics AS metrics
            LEFT JOIN previous_metrics AS previous ON TRUE
            ON CONFLICT (dag_id, run_id) DO UPDATE SET
                recorded_at = NOW(),
                row_count = EXCLUDED.row_count,
                city_count = EXCLUDED.city_count,
                latest_observation_date = EXCLUDED.latest_observation_date,
                observation_lag_days = EXCLUDED.observation_lag_days,
                latest_fetch_at = EXCLUDED.latest_fetch_at,
                dbt_test_status = EXCLUDED.dbt_test_status,
                observability_status = EXCLUDED.observability_status,
                warning_reasons = EXCLUDED.warning_reasons,
                city_metrics = EXCLUDED.city_metrics
            """,
            (dag_run.dag_id, dag_run.dag_id, dag_run.run_id, len(CITY_KEYS), len(CITY_KEYS)),
        )


def load_batch() -> int:
    # Dùng cùng ngày dữ liệu với các task ingest để chỉ nạp batch của lần chạy này.
    requested_date = get_requested_date()
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

    # Chạy Spark sau ingest để chỉ đọc đủ raw JSON của ngày này.
    spark_weather_batch = PythonOperator(
        task_id='spark_weather_batch',
        python_callable=run_spark_batch,
    )
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
    # Ghi snapshot chỉ khi toàn bộ dbt tests thành công.
    observability_snapshot = PythonOperator(
        task_id='record_observability_snapshot',
        python_callable=record_observability_snapshot,
    )

    # Sau ingest, Spark và load PostgreSQL có thể chạy song song; dbt chờ load hoàn tất.
    ingest_tasks >> spark_weather_batch
    ingest_tasks >> load >> dbt_run >> dbt_test >> observability_snapshot
