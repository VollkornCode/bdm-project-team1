import json
import requests
import time
import io
import sys
import os
import polars as pl
from datetime import datetime

# Import clients and utilities
from scripts.ingest import MinioClient, DeltaLakeClient
from scripts.util import util

# Import configuration constants
from scripts.conf import (
    POLARS_S3_STORAGE_OPTIONS, 
    DELTALAKE_TABLES,
    SPOON_KEY,
    SPOON_BASE_URL
)

class SpoonacularClient:
    ''' Client for interacting with the Spoonacular API. '''
    
    def __init__(self):
        self.base_url = SPOON_BASE_URL
        self.api_key = SPOON_KEY

    def fetch_random_recipes(self, number: int = 10) -> dict:
        ''' Fetches random recipes from the Spoonacular API. '''
        params = {
            "apiKey": self.api_key,
            "number": number,
            "includeNutrition": "true"
        }
        
        response = requests.get(self.base_url, params=params, timeout=30)
        
        # Handle API rate limits (429 Too Many Requests)
        if response.status_code == 429:
            print("Spoonacular API quota reached! Waiting 60 seconds...")
            time.sleep(60)
            return self.fetch_random_recipes(number)
            
        response.raise_for_status()
        return response.json()

'''
Note: Calling this endpoint requires 1 point and 0.01 points per recipe returned and 
      0.5 points per recipe returned if includeNutrition is set to true. 50 points per day.
'''
def init_fetch(minio_client: MinioClient, delta_client: DeltaLakeClient, recipe_count: int):
    
    ''' 
    Orchestrates the ingestion for Spoonacular:
    1. Fetches raw data from the API.
    2. Uploads the 100% raw JSON to the 'raw-data' bucket.
    3. Transfers the 100% content to the 'deltalake' bucket using Polars.
    '''
    
    # Initialize API Client
    spoon_client = SpoonacularClient()
    
    # Ensure buckets exists
    minio_client.create_buckets()

    print(f"--- Starting Spoonacular Ingestion: Requesting {recipe_count} recipes ---")

    try:
        # 1. Fetch data from API
        data = spoon_client.fetch_random_recipes(recipe_count)
        
        if not data.get("recipes"):
            print("No recipes found in the API response. Aborting.")
            return

        # 2. Define naming convention and paths
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"recipes_{timestamp}.json"
        object_key = f"spoonocular/{filename}"

        # 3. Upload the file to MinIO "raw-data" bucket.
        minio_client.upload_object(data, "raw-data", object_key)
        print(f"Success: Raw JSON uploaded to s3://raw-data/{object_key}")

        # 4. Upload the file to MinIO "deltalake" bucket.
        recipes_list = data.get("recipes", [])
        if recipes_list:
            delta_client.write_table(recipes_list, DELTALAKE_TABLES["SPOONOCULAR"], partition_by=["title"])
            print(f"Success: 100% of data registered in Delta Lake at {DELTALAKE_TABLES['SPOONOCULAR']}")

    except Exception as e:
        print(f"Critical error during Spoonacular ingestion: {e}")

    print("--- Spoonacular Ingestion Completed ---")