FROM apache/airflow:3.1.7

# Additional packages required by the lab DAGs
# (installed as the airflow user – no root needed)
RUN pip install --no-cache-dir \
        "deltalake" \
        "polars" \
        "pyarrow" \
        "duckdb" \
        "pandas" \
        "boto3" \
        "kafka-python==2.3.0"
