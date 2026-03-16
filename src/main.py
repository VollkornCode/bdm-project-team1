'''main.py: Main entry point for the data ingester.'''

import time
import os
import tempfile

import pandas as pd
import schedule

from config.conf import FAOSTAT_EU_COUNTRY_CODES, PROJECT_ROOT, DELTALAKE_TABLES
import util.clean_utils as clean_utils
from fetch import FaostatClient
from ingest import MinioClient, DeltaLakeClient

def init_data_load(minio_client: MinioClient, delta_client: DeltaLakeClient):
    '''On service startup, this function will load all historic datasets from the different APIs, clean them, and upload them to the data lake.'''
    # Create temporary directory if it doesn't exist
    tmp_dir = tempfile.mkdtemp(dir=PROJECT_ROOT)
    minio_client.create_buckets()  # Ensure the required buckets exist in MinIO.
    
    ### FAOSTAT Data
    faostat_client = FaostatClient()
    faostat_tmp_dir = os.mkdir(os.path.join(tmp_dir, "faostat"))
    # Fetch all available food CPI data for EU countries and save it as a CSV file in the temporary directory.
    food_cpi_data = faostat_client.get_food_cpi(country_codes=FAOSTAT_EU_COUNTRY_CODES)
    filename = f"food_cpi_init_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    food_cpi_data.to_csv(f"{tmp_dir}/{filename}")
    files = [(os.path.join(tmp_dir, f), f"faostat/{f}") for f in os.listdir(tmp_dir) if os.path.isfile(os.path.join(tmp_dir, f))]

    # Upload the CSV file to the MinIO data lake.
    for file, key in files:
        minio_client.upload_file(file, "raw-data", key)

    #Clean data and upload to deltalake
    cleaned_data = clean_utils.clean_food_cpi_data(f"s3://raw-data/faostat/{filename}")
    delta_client.write_table(cleaned_data, DELTALAKE_TABLES["FAOSTAT_FOOD_CPI"], partition_by=["Year"])

    # Remove temporary directory and its contents
    clean_utils.rmdir_recursively(tmp_dir)

def some_recurring_task():
    '''Example of a recurring task that could be scheduled to run periodically.'''
    print("This is a recurring task that runs every 5 seconds.")

schedule.every(5).seconds.do(some_recurring_task)

def main():
    minio_client = MinioClient()
    delta_client = DeltaLakeClient()

    init_data_load(minio_client, delta_client)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
    

if __name__ == "__main__":
    main()
