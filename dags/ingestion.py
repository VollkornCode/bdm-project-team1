from __future__ import annotations
import os
import json
import boto3
from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task

# Constants for your environment
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
ACCESS_KEY     = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
SECRET_KEY     = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME    = "landing-zone/temporal_landing"

@dag(
    dag_id="api_ingestion",
    schedule=timedelta(hours=1),
    start_date=datetime.now(tz=timezone.utc) - timedelta(days=1),
    catchup=False,
    tags=["project", "api", "s3"]
)
def api_to_s3_pipeline():

    @task()
    def fetch_and_upload(api_name: str, url: str):
        import requests
        
        # 1. Get Data
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        # 2. Setup S3 Client
        s3 = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )

        # 3. Create a unique filename (using execution date is best practice)
        filename = f"{api_name}/{datetime.now().strftime('%Y%m%d_%H%M')}.json"

        # 4. Upload
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=filename,
            Body=json.dumps(data)
        )
        
        return {"file_saved": filename}

    # Execute for your APIs
    fetch_and_upload("users", "https://api.example.com/users")
    fetch_and_upload("orders", "https://api.example.com/orders")

api_to_s3_pipeline()