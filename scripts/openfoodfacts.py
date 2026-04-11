import json
import requests
import time
import io
import os
import sys
import polars as pl
from datetime import datetime

from scripts.ingest import MinioClient, DeltaLakeClient
from scripts.util import util

# Import configuration constants
from scripts.conf import (
    POLARS_S3_STORAGE_OPTIONS, 
    DELTALAKE_TABLES,
    OFF_BASE_URL,
    OFF_BASE_HEADER,
)

class OpenFoodFactsClient:
    ''' Client for interacting with the OpenFoodFacts API. '''
    
    def __init__(self):
        self.base_url = OFF_BASE_URL
        self.headers = OFF_BASE_HEADER
        self.state_file = "ingestion_state.json"

    def _get_last_processed_page(self) -> int:
        ''' Reads the local state file to determine where to resume. '''
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, "r") as f:
                    state = json.load(f)
                    return state.get("last_page", 0)
            except Exception as e:
                print(f"Error reading state file: {e}")
        return 0

    def _save_current_page(self, page: int):
        ''' Persists the current page number to maintain ingestion state. '''
        state = {
            "last_page": page,
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        with open(self.state_file, "w") as f:
            json.dump(state, f, indent=4)

    def fetch_page(self, page: int, page_size: int) -> dict:
        ''' Fetches product data for a specific page. '''
        params = {
            "sort_by": "unique_scans_n",
            "page": page,
            "page_size": page_size,
            "json": "true",
            "fields": "code,product_name,brands,nutriments,countries,categories,image_url"
        }
        
        response = requests.get(self.base_url, params=params, headers=self.headers, timeout=30)
        
        if response.status_code == 429:
            print("429 Rate Limit reached! Waiting 60 seconds...")
            time.sleep(60)
            return self.fetch_page(page, page_size)
            
        response.raise_for_status()
        return response.json()

'''
Note: The OpenFoodFacts API has a limit of 10 requests per minute for search queries.
'''
def init_fetch(minio_client: MinioClient, delta_client: DeltaLakeClient, pages_to_download: int, page_size: int):
    
    '''
    Orchestrates the ingestion for OpenFoodFacts:
    1. Fetches raw data from the API.
    2. Uploads the 100% raw JSON to the 'raw-data' bucket.
    3. Transfers the 100% content to the 'deltalake' bucket using Polars.
    '''
    
    # Initialize Clients
    off_client = OpenFoodFactsClient()
    
    # Ensure buckets exists
    minio_client.create_buckets()
    
    # Resume logic from state file
    last_page = off_client._get_last_processed_page()
    start_page = last_page + 1
    end_page = last_page + pages_to_download

    print(f"--- Starting OFF Ingestion: Resuming from page {start_page} ---")

    for current_page in range(start_page, end_page + 1):
        try:
            # 1. Fetch data from API
            data = off_client.fetch_page(current_page, page_size)
            
            if not data.get("products"):
                print(f"No recipes found in the API response. Aborting.")
                break
            
            # 2. Define naming convention and paths
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"off_page_{current_page}_{timestamp}.json"
            object_key = f"openfoodfacts/{filename}"
            s3_path = f"s3://raw-data/{object_key}"
            
            # 3. Upload the file to MinIO "raw-data" bucket.
            minio_client.upload_file(data, "raw-data", object_key)

            # 4. Upload the file to MinIO "deltalake" bucket.
            df_full = pl.read_json(s3_path, storage_options=POLARS_S3_STORAGE_OPTIONS)
            delta_client.write_table(df_full, DELTALAKE_TABLES["OPENFOODFACTS"], partition_by=["brands"])
            
            off_client._save_current_page(current_page)
            print(f"Success: Page {current_page} uploaded to s3://raw-data/{object_key}")

        except Exception as e:
            print(f"Critical error on page {current_page}: {e}")
            break

    print("--- OpenFoodFacts Ingestion Completed ---")