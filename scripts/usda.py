import json
import requests
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
    USDA_KEY,
    USDA_BASE_URL
)

class USDAClient:
    ''' Client for interacting with the USDA FoodData Central API. '''
    
    def __init__(self):
        self.base_url = USDA_BASE_URL
        self.api_key = USDA_KEY

    def search_foods(self, query: str, page_size: int = 10) -> dict:
        ''' 
        Searches for food items in the USDA database using a POST request.
        Captures the 100% of the JSON response.
        '''
        payload = {
            "query": query,
            "pageSize": page_size,
            "dataType": ["Foundation", "Survey (FNDDS)"],
            "api_key": self.api_key
        }
        
        # USDA API often requires the API key both in payload and as a parameter
        response = requests.post(
            self.base_url, 
            json=payload, 
            params={"api_key": self.api_key}, 
            timeout=30
        )
        
        response.raise_for_status()
        return response.json()

'''
Note: The USDA API has a limit of 1.000 requests per hour per IP address.
'''
def init_fetch(minio_client: MinioClient, delta_client: DeltaLakeClient, query: str):
    
    ''' 
    Orchestrates the USDA Ingestion:
    1. Fetches raw food data from the API.
    2. Persists the 100% raw JSON in the 'raw-data' bucket.
    3. Transfers the 100% content to the 'deltalake' bucket using Polars.
    '''
    
    # Initialize API Client
    usda_client = USDAClient()
    
    # Ensure buckets exists
    minio_client.create_buckets()

    print(f"--- Starting USDA Ingestion: Searching for '{query}' ---")

    try:
        # 1. Fetch data from API
        data = usda_client.search_foods(query)
        
        if not data.get("foods"):
            print(f"No foods found for query: {query}. Aborting.")
            return

        # 2. Define naming convention and paths
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        clean_query = query.replace(" ", "_").lower()
        filename = f"usda_{clean_query}_{timestamp}.json"
        object_key = f"usda/{filename}"

        # 3. Upload the file to MinIO "raw-data" bucket.
        minio_client.upload_object(data, "raw-data", object_key)
        print(f"Step 1: Raw JSON uploaded to s3://raw-data/{object_key}")

        # 4. Upload the file to MinIO "deltalake" bucket.
        foods_list = data.get("foods", [])
        if foods_list:
            delta_client.write_table(foods_list, DELTALAKE_TABLES["USDA"], partition_by=None)        
            print(f"Step 2: 100% of content registered in Delta Lake at {DELTALAKE_TABLES['USDA']}")

    except Exception as e:
        print(f"Critical error during USDA ingestion: {e}")

    print("--- USDA Ingestion Completed ---")