import os
import boto3
import zipfile
from botocore.exceptions import ClientError

# Import configuration constants from the central config file.
from config.conf import (
    KAGGLE_USERNAME, 
    KAGGLE_KEY, 
    KAGGLE_5k_DATASET,
    MINIO_ENDPOINT, 
    MINIO_ACCESS_KEY, 
    MINIO_SECRET_KEY
)

# Ingests specific CSV files from the Nutrition5k Kaggle dataset into MinIO.
def ingest_5k():

    # Set system environment variables explicitly for the Kaggle API library.
    os.environ['KAGGLE_USERNAME'] = KAGGLE_USERNAME
    os.environ['KAGGLE_KEY'] = KAGGLE_KEY

    # Validate that credentials are provided in the config.
    if not os.environ['KAGGLE_USERNAME'] or not os.environ['KAGGLE_KEY']:
        print("Error: Kaggle credentials not found in configuration.")
        return

    # Local import to ensure Kaggle reads the environment variables just set.
    from kaggle.api.kaggle_api_extended import KaggleApi
    
    # Authenitace with Kaggle API.
    try:
        api = KaggleApi()
        api.authenticate()
        print("Kaggle authentication: OK")
    except Exception as e:
        print(f"Critical authentication failure: {e}")
        return
    
    # MinIO Client configuration using centralized variables.
    s3 = boto3.client(
        "s3", 
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    
    bucket_name = "landing-zone"
    target_folder = "temporal_landing/5k/"
    download_path = "./temp_kaggle_files"

    if not os.path.exists(download_path):
        os.makedirs(download_path)

    # Specific CSV files required for this dataset.
    files_to_ingest = ["dish_ingredients.csv", "dish_nutrition_values.csv", "ingredients_metadata.csv"]
    
    print(f"--- Starting selective ingestion process from {KAGGLE_5k_DATASET} ---")

    for file_name in files_to_ingest:
        try:
            # Download the file from Kaggle.
            print(f"Downloading {file_name}...")
            api.dataset_download_file(KAGGLE_5k_DATASET, file_name, path=download_path)
            
            local_file_path = os.path.join(download_path, file_name)
            zip_path = local_file_path + ".zip"

            # Check if Kaggle downloaded the file as a zip.
            if os.path.exists(zip_path):
                print(f"Extracting {zip_path}...")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(download_path)
                os.remove(zip_path) # Remove zip after extraction

            # Verify the unzipped or direct file exists before uploading to MinIO.
            if os.path.exists(local_file_path):
                object_key = f"{target_folder}{file_name}"
                print(f"Uploading to MinIO: {object_key}")
                s3.upload_file(local_file_path, bucket_name, object_key)
                
                # Clean up local temporary files.
                os.remove(local_file_path)
                print(f"Success: {file_name} processed and removed locally.")
            else:
                print(f"Warning: Expected file {file_name} was not found after download/extraction.")
            
        except Exception as e:
            print(f"Error processing {file_name}: {e}")

if __name__ == "__main__":
    ingest_5k()