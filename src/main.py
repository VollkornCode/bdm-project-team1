'''main.py: Main entry point for the data ingester.'''

import os
import tempfile

import pandas as pd

from config.conf import FAOSTAT_EU_COUNTRY_CODES, PROJECT_ROOT, DELTALAKE_TABLES
import util.clean_utils as clean_utils
from fetch import FaostatClient
from ingest import MinioClient, DeltaLakeClient

TEMP_DIR = tempfile.mkdtemp(dir=PROJECT_ROOT)


def main():
    minio_client = MinioClient()
    delta_client = DeltaLakeClient()
    faostat_client = FaostatClient()

    # Fetch Food CPI data for EU countries from 2020 to 2025 and save it as a CSV file in the temporary directory.
    food_cpi_data = faostat_client.get_food_cpi(country_codes=FAOSTAT_EU_COUNTRY_CODES, start_year=2020, end_year=2025)
    print(food_cpi_data.head(3))
    food_cpi_data.to_csv(f"{TEMP_DIR}/food_cpi.csv")
    files = [(os.path.join(TEMP_DIR, f), f"faostat/{f}") for f in os.listdir(TEMP_DIR) if os.path.isfile(os.path.join(TEMP_DIR, f))]

    minio_client.create_buckets()  # Ensure the required buckets exist in MinIO.

    # Upload the CSV file to the MinIO data lake.
    for file, key in files:
        minio_client.upload_file(file, "raw-data", key)

    #Clean data and upload to deltalake
    cleaned_data = clean_utils.clean_food_cpi_data(f"s3://raw-data/faostat/food_cpi.csv")
    #data_as_parquet = clean_utils.df_to_parquet(cleaned_data, f"{TEMP_DIR}/cleaned_food_cpi.parquet")
    delta_client.write_table(cleaned_data, DELTALAKE_TABLES["FAOSTAT_FOOD_CPI"], partition_by=["Year"])

    # Clean up temporary files after uploading.
    for file in os.listdir(TEMP_DIR):
        os.remove(file)
        os.removedirs(TEMP_DIR)

if __name__ == "__main__":
    print(f"TEMP_DIR: {TEMP_DIR}")
    main()
