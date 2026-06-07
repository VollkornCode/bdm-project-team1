''' Fetching data from the web. Provides classes and methods for all different APIs needed for the project.'''

from datetime import datetime, timedelta
from io import StringIO
import tempfile
import os
import sys
import requests
import pandas as pd
import polars as pl

from scripts.ingest import MinioClient, DeltaLakeClient
from scripts.util import util

from scripts.conf import (PROJECT_ROOT,
                         POLARS_S3_STORAGE_OPTIONS, 
                         DELTALAKE_TABLES,
                         FAOSTAT_BASE_URL,
                         FAOSTAT_TOKEN_TIMEOUT,
                         FAOSTAT_EU_COUNTRY_CODES,
                         FAOSTAT_ITEM_CODES,
                         FAOSTAT_DOMAIN_CODES,
                         FAOSTAT_PWD,
                         FAOSTAT_UNAME)


### FAOSTAT API Wrapper
# See https://www.fao.org/faostat/en/#developer-portal for more details on the API.
class FaostatClient:
    ''' Client for interacting with the FAOSTAT API. '''
    
    def __init__(self):
            self.auth_token = self._get_faostat_auth_token()
            self.auth_token_date = datetime.now()

    def _get_faostat_auth_token(self) -> str:
        ''' Fetches an authentication token from the FAOSTAT API. '''
        url = f"{FAOSTAT_BASE_URL}/auth/login"
        payload = {
            "username": FAOSTAT_UNAME,
            "password": FAOSTAT_PWD
        }
        headers = {
            "authorization": "Bearer ",
            "content-type": "application/x-www-form-urlencoded"
        }

        response = requests.post(url, data=payload, headers=headers, timeout=3)
        response.raise_for_status()  # Raise an exception for HTTP errors
        try:
            token = response.json()["AuthenticationResult"]["AccessToken"]
        except Exception as e:
            raise Exception(f"Failed to parse authentication token from response: {e}")
        return token

    def _refresh_token_if_needed(self):
        ''' Refreshes the authentication token if it is older than FAOSTAT_TOKEN_TIMEOUT. '''
        if datetime.now() - self.auth_token_date > timedelta(seconds=FAOSTAT_TOKEN_TIMEOUT-5):
            self.auth_token = self._get_faostat_auth_token()
            self.auth_token_date = datetime.now()

    def get_food_cpi(self, country_codes: str, start_year: int | None = None, end_year: int | None = None) -> pd.DataFrame:
        ''' Fetches the food consumer price index (CPI) data from the FAOSTAT API for the specified country codes and year range. 
            Returns csv as Dataframe'''
        self._refresh_token_if_needed()
        headers = {"Authorization": f"Bearer {self.auth_token}"}
        domain_code = FAOSTAT_DOMAIN_CODES["CONSUMER_PRICES"]
        food_cpi_code = FAOSTAT_ITEM_CODES["FOOD_CPI"]
        params = {
            "item": food_cpi_code,
            "area": country_codes,
            "output_type": "csv"
        }
        if start_year is not None and end_year is not None:
            year_codes = ','.join([str(year) for year in range(start_year, end_year + 1)])
            params["year"] = year_codes

        url = f"{FAOSTAT_BASE_URL}/en/data/{domain_code}"
        print(f"Fetching food consumer price indices for the EU countries...")
        respone_start_time = datetime.now()
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response_end_time = datetime.now()
        print(f"Response time: {response_end_time - respone_start_time}")
        response.raise_for_status()  # Raise an exception for HTTP errors
        response_content = response.content
        # Parse CSV to Dataframe from response bytes
        try:
            df = pd.read_csv(StringIO(response_content.decode('utf-8')))
        except Exception as e:
            raise Exception(f"Failed to parse CSV data from response: {e}")
        return df

def init_fetch(minio_client: MinioClient, delta_client: DeltaLakeClient):
    '''On service startup, this function will load all historic datasets from the different APIs, clean them, and upload them to the data lake.'''
    # Create temporary directory if it doesn't exist
    tmp_dir = tempfile.mkdtemp(dir=PROJECT_ROOT)
    minio_client.create_buckets()  # Ensure the required buckets exist in MinIO.
    
    ### FAOSTAT Data
    faostat_client = FaostatClient()
    # Fetch all available food CPI data for EU countries and save it as a CSV file in the temporary directory.
    food_cpi_data = faostat_client.get_food_cpi(country_codes=FAOSTAT_EU_COUNTRY_CODES)
    filename = f"food_cpi_init_{pd.Timestamp.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    food_cpi_data.to_csv(f"{tmp_dir}/{filename}")
    files = [(os.path.join(tmp_dir, f), f"faostat/{f}") for f in os.listdir(tmp_dir) if os.path.isfile(os.path.join(tmp_dir, f))]

    # Upload the CSV file to the MinIO data lake.
    for file, key in files:
        minio_client.upload_file(file, "raw-data", key)

    # Clean data and upload to deltalake - This only happens in P2, not necessary at this time
    # cleaned_data = clean_utils.clean_food_cpi_data(f"s3://raw-data/faostat/{filename}")

    delta_df = pl.from_dataframe(food_cpi_data)
    delta_client.write_table(delta_df, DELTALAKE_TABLES["FAOSTAT_FOOD_CPI"], partition_by=["Year"] if "Year" in delta_df.columns else None)

    # Remove temporary directory and its contents
    util.rmdir_recursively(tmp_dir)

def clean_food_cpi_data(s3_file_path: str) -> pl.DataFrame:
    ''' Cleans the raw food consumer price index (CPI) data from the FAOSTAT API. 
        Reads the raw CSV data from the specified S3 file path, drops unnecessary columns, and renames the "Area" column to "Country". 
        Returns the cleaned data as a Polars DataFrame.'''
    df = pl.read_csv(s3_file_path, storage_options=POLARS_S3_STORAGE_OPTIONS)
    df = df.drop("Domain Code", "Area Code", "Year Code", "Item Code", "Months Code", "Element Code", "Element", "Unit", "Flag", "Flag Description", "Note")
    df = df.rename({"Area": "Country"})
    return df


def clean_food_cpi_frame(df: pl.DataFrame) -> pl.DataFrame:
    drop_candidates = [
        "Domain Code",
        "Area Code",
        "Year Code",
        "Item Code",
        "Months Code",
        "Element Code",
        "Element",
        "Unit",
        "Flag",
        "Flag Description",
        "Note",
    ]
    existing_drop_columns = [column for column in drop_candidates if column in df.columns]
    if existing_drop_columns:
        df = df.drop(existing_drop_columns)

    if "Area" in df.columns:
        df = df.rename({"Area": "Country"})

    return df


def init_trusted_faostat(minio_client: MinioClient, delta_client: DeltaLakeClient):
    minio_client.create_buckets()

    landing_path = DELTALAKE_TABLES["FAOSTAT_FOOD_CPI"]
    trusted_path = DELTALAKE_TABLES["FAOSTAT_FOOD_CPI_TRUSTED"]

    df = delta_client.read_table(landing_path)
    cleaned_df = df

    delta_client.write_table(cleaned_df, trusted_path, partition_by=["Year"] if "Year" in cleaned_df.columns else None)


def init_exploitation_faostat(minio_client: MinioClient, delta_client: DeltaLakeClient):
    minio_client.create_buckets()

    trusted_path = DELTALAKE_TABLES["FAOSTAT_FOOD_CPI_TRUSTED"]
    exploitation_path = DELTALAKE_TABLES["FAOSTAT_FOOD_CPI_EXPLOITATION"]

    df = delta_client.read_table(trusted_path)
    df = clean_food_cpi_frame(df)

    group_columns = [column for column in ["Country", "Year"] if column in df.columns]
    if not group_columns:
        raise ValueError("FAOSTAT exploitation requires at least one grouping column")

    aggregated_columns = []

    if "Value" in df.columns:
        aggregated_columns.extend([
            pl.col("Value").cast(pl.Float64, strict=False).mean().alias("avg_value"),
            pl.col("Value").cast(pl.Float64, strict=False).min().alias("min_value"),
            pl.col("Value").cast(pl.Float64, strict=False).max().alias("max_value"),
        ])

    aggregated_columns.append(pl.len().alias("row_count"))

    exploitation_df = df.group_by(group_columns).agg(aggregated_columns).sort(group_columns)
    delta_client.write_table(exploitation_df, exploitation_path, partition_by=["Year"] if "Year" in exploitation_df.columns else None)