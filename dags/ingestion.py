from __future__ import annotations
import os
import sys
from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task


sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.ingest import MinioClient, DeltaLakeClient

import scripts.openfoodfacts as openfoodfacts
import scripts.spoonocular as spoonocular
import scripts.usda as usda
import scripts.faostat as faostat
import scripts.consumer as kafka_consumer
import scripts.openfood_prices as openfoodfacts_prices
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from scripts.conf import (
    OFF_PAGES,
    OFF_PAGE_SIZE,
    RECIPE_COUNT
)

@dag(
    dag_id="ingest_openfoodfacts_recipes_api",
    schedule=timedelta(minutes=30),
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["project", "food_data", "deltalake", "minio"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    }
)
def openfoodfacts_recipes_airflow():

    @task()
    def ingest_openfoodfacts_recipes():
        m_client = MinioClient()
        d_client = DeltaLakeClient()
        openfoodfacts.init_fetch(m_client, d_client, OFF_PAGES, OFF_PAGE_SIZE)
    
    ingest_openfoodfacts_recipes()

@dag(
    dag_id="ingest_openfoodfacts_prices_api",
    schedule=timedelta(minutes=30),
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["project", "food_data", "deltalake", "minio"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    }
)
def openfoodfacts_prices_airflow():

    @task()
    def ingest_openfoodfacts_prices():
        m_client = MinioClient()
        d_client = DeltaLakeClient()
        openfoodfacts_prices.init_fetch(m_client, d_client, OFF_PAGES, OFF_PAGE_SIZE)
    
    ingest_openfoodfacts_prices()

@dag(
    dag_id="ingest_spoonacular_api",
    schedule=timedelta(hours=6),
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["project", "food_data", "deltalake", "minio"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    }
)
def spoonacular_airflow():

    @task()
    def ingest_spoonacular():
        m_client = MinioClient()
        d_client = DeltaLakeClient()
        spoonocular.init_fetch(m_client, d_client, RECIPE_COUNT)

    ingest_spoonacular()

@dag(
    dag_id="process_trusted_images",
    schedule=timedelta(hours=1),
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["project", "trusted_zone", "pyspark", "minio"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
)
def trusted_images_airflow():

    process_images = SparkSubmitOperator(
        task_id="process_images",
        application="/opt/airflow/scripts/spark.py",
        conn_id="spark_default",
        name="TrustedImagePipeline",
        application_args=[],
        env_vars={
            "PYTHONPATH": "/opt/airflow"
        },
        jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    )

    process_images

trusted_images_airflow()

@dag(
    dag_id="explotation_images",
    schedule=timedelta(hours=1),
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["project", "explotation_zone", "pyspark", "minio", "milvus"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
)
def explotation_images_airflow():

    explotation_images = SparkSubmitOperator(
        task_id="explotation_images",
        application="/opt/airflow/scripts/sparkM.py",
        conn_id="spark_default",
        name="ExplotationImagePipeline",
        application_args=[],
        env_vars={
            "PYTHONPATH": "/opt/airflow"
        },
        jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    )

    explotation_images

explotation_images_airflow()

@dag(
    dag_id="ingest_usda_api",
    schedule=timedelta(hours=1),
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["project", "food_data", "deltalake", "minio"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    }
)
def usda_airflow():

    @task()
    def ingest_usda():
        m_client = MinioClient()
        d_client = DeltaLakeClient()
        usda.init_fetch(m_client, d_client)

    ingest_usda()

@dag(
    dag_id="ingest_faostat_api",
    schedule=timedelta(hours=1),
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["project", "food_data", "deltalake", "minio"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    }
)
def faostat_airflow():

    @task()
    def ingest_faostat():
        m_client = MinioClient()
        d_client = DeltaLakeClient()
        faostat.init_fetch(m_client, d_client)

    ingest_faostat()

@dag(
    dag_id="ingest_kafka",
    schedule=timedelta(seconds=10),
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["project", "food_data", "deltalake", "minio"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
)
def kafka_airflow():

    @task()
    def ingest_kafka():
        m_client = MinioClient()
        kafka_consumer.init_fetch(m_client)

    ingest_kafka()

@dag(
    dag_id="trusted_parquet",
    schedule=timedelta(hours=1),
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["project", "trusted_zone", "pyspark", "minio", "parquet"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
)
def trusted_parquet_airflow():

    trusted_parquet = SparkSubmitOperator(
        task_id="trusted_parquet",
        application="/opt/airflow/scripts/sparkJSON.py",
        conn_id="spark_default",
        name="ExplotationImagePipeline",
        application_args=[],
        env_vars={
            "PYTHONPATH": "/opt/airflow"
        },
        jars="/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar"
    )

    trusted_parquet

trusted_parquet_airflow()

openfoodfacts_recipes_airflow()
openfoodfacts_prices_airflow()
spoonacular_airflow()
trusted_images_airflow()
explotation_images_airflow()
usda_airflow()
faostat_airflow()
kafka_airflow()
trusted_parquet_airflow()