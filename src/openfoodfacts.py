import os
import json
import requests
import boto3
import time
from datetime import datetime

# Import configuration constants from the central config file
from config.conf import (
    OFF_BASE_URL,
    OFF_BASE_HEADER,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY
)

'''
Note: The OpenFoodFacts API has a limit of 10 requests per minute for search queries.
'''
# ingests product data from OpenFoodFacts and uploads it to MinIO
def ingest_off_automatic(pages=3, page_size=20):

    # MinIO Client configuration using centralized variables.
    s3 = boto3.client("s3", 
                      endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY)
    
    bucket_name = "landing-zone"
    target_folder = "temporal_landing/openfoodfacts/"

    print(f"--- Starting Automatic Ingestion (Rate Limit: 10 req/min) ---")

    # Page loop
    for current_page in range(1, pages + 1):
        params = {
            "sort_by": "unique_scans_n", # Sort by popularity/scans
            "page": current_page,
            "page_size": page_size,
            "json": "true",
            "fields": "code,product_name,brands,nutriments,countries,categories"
        }

        max_retries = 2  # Attempt each page up to 2 times if it fails
        for attempt in range(max_retries):
            try:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Requesting page {current_page} (Attempt {attempt+1})...")
                
                # Using constants from config.conf for URL and headers
                response = requests.get(OFF_BASE_URL, params=params, headers=OFF_BASE_HEADER, timeout=30)
                
                if response.status_code == 429:
                    print("429 Rate Limit reached! Waiting 60 seconds...")
                    time.sleep(60)
                    continue # Retry the same page

                response.raise_for_status()
                data = response.json()

                # MinIO Upload Logic
                # Files are named using a timestamp to prevent overwriting
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                file_name = f"off_page_{current_page}_{timestamp}.json"
                object_key = f"{target_folder}{file_name}"
                
                s3.put_object(
                    Bucket=bucket_name,
                    Key=object_key,
                    Body=json.dumps(data, indent=4),
                    ContentType='application/json'
                )
                print(f"Success: Page {current_page} synchronized to MinIO.")
                break # Success: move to the next page

            except (requests.exceptions.RequestException, Exception) as e:
                print(f"Error on attempt {attempt+1} for page {current_page}: {e}")
                if attempt < max_retries - 1:
                    print("Waiting 15 seconds before retrying...")
                    time.sleep(15)
                else:
                    print(f"Page {current_page} failed after {max_retries} attempts. Skipping...")

    print("--- Ingestion process completed ---")

if __name__ == "__main__":
    # Download 3 test pages (approx. 60 products with page_size=20)
    ingest_off_automatic(pages=3, page_size=20)