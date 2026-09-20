FROM apache/airflow:3.3.2-python3.12

USER airflow

WORKDIR /opt/airflow/finstream

COPY --chown=airflow:root pyproject.toml ./
COPY --chown=airflow:root src ./src
COPY --chown=airflow:root dbt ./dbt
COPY --chown=airflow:root docker/airflow/metadata_connection.py ./docker/airflow/metadata_connection.py

RUN pip install --no-cache-dir "apache-airflow==3.3.2" -e ".[airflow]" \
    && pip check
