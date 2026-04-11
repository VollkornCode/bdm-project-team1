from __future__ import annotations
import os
from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task

from src.ingest import MinioClient, DeltaLakeClient

import scripts.openfoodfacts as openfoodfacts
import scripts.spoonocular as spoonocular
import scripts.usda as usda
import scripts.faostat as faostat

from conf import (
    OFF_PAGES,
    OFF_PAGE_SIZE,
    RECIPE_COUNT,
    QUERY
)

@dag(
    dag_id="food_data_lakehouse_ingestion",
    schedule=timedelta(hours=24),
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["project", "food_data", "deltalake", "minio"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
)
def api_to_lakehouse_pipeline():

    @task()
    def ingest_openfoodfacts():
        m_client = MinioClient()
        d_client = DeltaLakeClient()
        openfoodfacts.init_fetch(m_client, d_client, OFF_PAGES, OFF_PAGE_SIZE)

    @task()
    def ingest_spoonacular():
        m_client = MinioClient()
        d_client = DeltaLakeClient()
        spoonocular.init_fetch(m_client, d_client, RECIPE_COUNT)

    @task()
    def ingest_usda():
        m_client = MinioClient()
        d_client = DeltaLakeClient()
        usda.init_fetch(m_client, d_client, QUERY)

    @task()
    def ingest_faostat():
        m_client = MinioClient()
        d_client = DeltaLakeClient()
        faostat.init_fetch_food_cpi(m_client, d_client)

    [
        ingest_openfoodfacts(),
        ingest_spoonacular(),
        ingest_usda(),
        ingest_faostat()
    ]

api_to_lakehouse_pipeline()