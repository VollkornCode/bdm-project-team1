import os
import json
import requests
import boto3
from datetime import datetime

# Import configuration constants from the central config file.
from config.conf import (
    SPOON_KEY,
    SPOON_BASE_URL,
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY
)

'''
Note: Calling this endpoint requires 1 point and 0.01 points per recipe returned and 
0.5 points per recipe returned if includeNutrition is set to true. 
'''

# Fetches random recipes from Spoonacular API and uploads the resulting JSON directly to MinIO
def ingest_spoonacular():
    
    # MinIO Client configuration using centralized variables.
    s3 = boto3.client("s3", 
                      endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY)
    
    bucket_name = "landing-zone"
    target_folder = "temporal_landing/spoon/"
    
    # Request Parameters
    params = {
        "apiKey": SPOON_KEY,
        "number": 5,
        "includeNutrition": "true"
    }

    print(f"Requesting {params['number']} random recipes from Spoonacular...")

    try:
        # 1. Execute GET request
        # Uses SPOON_BASE_URL from configuration
        response = requests.get(SPOON_BASE_URL, params=params)
        response.raise_for_status() # Raise error for 4xx or 5xx responses
        
        data = response.json()

        # 2. Prepare file for MinIO
        # Use timestamp to ensure unique filenames for each execution
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_name = f"random_recipes_{timestamp}.json"
        object_key = f"{target_folder}{file_name}"

        # Convert dictionary to JSON string
        json_data = json.dumps(data, indent=4)

        # 3. Upload to MinIO
        # Using put_object to upload data directly from memory without local storage
        print(f"Uploading results to MinIO: {object_key}")
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json_data,
            ContentType='application/json'
        )

        print("--- Spoonacular ingestion completed successfully ---")

    except requests.exceptions.HTTPError as err:
        print(f"Spoonacular API request error: {err}")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    ingest_spoonacular()