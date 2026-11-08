FROM python:3.12-slim

RUN pip install --no-cache-dir "dbt-core==1.8.9" "dbt-clickhouse==1.8.9"

WORKDIR /dbt
ENV DBT_PROFILES_DIR=/dbt

CMD ["sh", "-c", "dbt run && dbt test"]
