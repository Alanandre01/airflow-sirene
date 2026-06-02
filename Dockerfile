FROM apache/airflow:3.2.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libxml2-dev \
    libxmlsec1-dev \
    libxmlsec1-openssl \
    pkg-config \
    && python -m pip install --no-cache-dir xmlsec \
    && python -m pip install --no-cache-dir \
       "apache-airflow-providers-snowflake==6.13.0" \
       "apache-airflow-providers-amazon==9.29.0" \
       "apache-airflow-providers-databricks==7.15.0" \
       dbt-core \
       dbt-snowflake \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
