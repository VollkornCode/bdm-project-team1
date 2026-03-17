import os
import json
import requests
import boto3
from datetime import datetime

# Import configuration constants from the central config file
from config.conf import (
    USDA_KEY,
    USDA_BASE_URL,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY
)

# Searches for food items in the USDA database and uploads the results directly to MinIO
def ingest_usda(query="apple"):

    # MinIO Client configuration using centralized variables.
    s3 = boto3.client("s3", 
                      endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY)
    
    bucket_name = "landing-zone"
    target_folder = "temporal_landing/usda/"
    
    # Using POST to allow for complex filters in the payload as seen in original usda.py
    payload = {
        "query": query,
        "pageSize": 10, # Number of results to return
        "dataType": ["Foundation", "Survey (FNDDS)"], # Reliable nutritional data types
        "api_key": USDA_KEY
    }
    
    print(f"Searching USDA foods for term: '{query}'...")

    try:
        # 1. Request to USDA API
        # The base URL is managed centrally in conf.py
        response = requests.post(USDA_BASE_URL, json=payload, params={"api_key": USDA_KEY})
        response.raise_for_status()
        
        data = response.json()

        # 2. Prepare Filename
        # Using a timestamp to ensure unique files and prevent overwriting
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        clean_query = query.replace(" ", "_").lower()
        file_name = f"search_{clean_query}_{timestamp}.json"
        object_key = f"{target_folder}{file_name}"

        # 3. Upload to MinIO
        # Using put_object to stream data directly from memory without saving local files
        print(f"Uploading {len(data.get('foods', []))} results to MinIO: {object_key}")
        
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(data, indent=4),
            ContentType='application/json'
        )

        print("--- USDA ingestion completed successfully ---")

    except Exception as e:
        print(f"Error during USDA ingestion: {e}")

if __name__ == "__main__":
    # Example: Change "apple" to any ingredient needed for your analysis
    ingest_usda("apple")