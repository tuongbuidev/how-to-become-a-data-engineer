FROM apache/airflow:3.3.2-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="/home/airflow/spark-venv/bin:${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=/home/airflow/spark-venv/bin/python

# Tách môi trường dbt khỏi các dependency bị Airflow ghim phiên bản.
COPY --chown=airflow:root airflow/requirements.txt /tmp/airflow-requirements.txt
RUN mkdir -p /opt/airflow/dbt /opt/airflow/plugins \
    && pip install --no-cache-dir -r /tmp/airflow-requirements.txt \
    && python -m venv /home/airflow/spark-venv \
    && /home/airflow/spark-venv/bin/pip install --no-cache-dir pyspark==3.5.3 \
    && python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-postgres==1.9.1 \
    && pip check \
    && /home/airflow/dbt-venv/bin/pip check
