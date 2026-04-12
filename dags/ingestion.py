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

openfoodfacts_recipes_airflow()
openfoodfacts_prices_airflow()
spoonacular_airflow()
usda_airflow()
faostat_airflow()
kafka_airflow()