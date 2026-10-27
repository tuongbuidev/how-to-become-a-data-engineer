FROM apache/airflow:3.3.2

# Tách môi trường dbt khỏi các dependency bị Airflow ghim phiên bản.
COPY --chown=airflow:root airflow/requirements.txt /tmp/airflow-requirements.txt
RUN mkdir -p /opt/airflow/dbt /opt/airflow/plugins \
    && pip install --no-cache-dir -r /tmp/airflow-requirements.txt \
    && python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-postgres==1.9.1 \
    && pip check \
    && /home/airflow/dbt-venv/bin/pip check
