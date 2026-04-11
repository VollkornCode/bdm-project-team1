''' File providing methods for ingesting fetched data into the data lake hosted on minio''' 

from deltalake import DeltaTable, write_deltalake
import duckdb
import polars as pl
import boto3
import json
import os
import sys
from botocore.exceptions import ClientError

class MinioClient:
    ''' Client for interacting with the MinIO data lake. '''
    
    def __init__(self):
        # Import internally to ensure the latest config from conf.py is used
        from scripts.conf import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
        
        self.endpoint = MINIO_ENDPOINT
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        # Verify connection immediately
        try:
            self.s3_client.list_buckets()
        except Exception as e:
            print(f"Error connecting to MinIO at {self.endpoint}: {e}")

    def create_buckets(self):
        #Create the two buckets our lab uses.
        # MinIO raises BucketAlreadyOwnedByYou if the bucket exists — that is fine.
        for bucket in ["raw-data", "deltalake"]:
            try:
                self.s3_client.create_bucket(Bucket=bucket)
                print(f"Created  : s3://{bucket}")
            except ClientError as e:
                code_ = e.response["Error"]["Code"]
                if code_ in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                    print(f"Exists   : s3://{bucket}  (nothing to do)")
                else:
                    raise

        # Confirm both are visible.
        buckets = [b["Name"] for b in self.s3_client.list_buckets()["Buckets"]]
        print(f"\nActive buckets: {buckets}")


    def upload_file(self, file_path: str, bucket_name: str, key: str) -> bool:
        ''' Uploads a file to the specified bucket in MinIO. '''
        try:
            self.s3_client.upload_file(file_path, bucket_name, key)
            print(f"File {file_path} uploaded to bucket {bucket_name} as {key}.")
            return True
        except ClientError as e:
            print(f"Failed to upload file {file_path} to bucket {bucket_name}: {e}")
            return False
        
class DeltaLakeClient:
    ''' Client for interacting with Delta Lake tables. '''
    
    def __init__(self):
        # Ensure it reads the latest storage options (endpoint, keys, etc.)
        from scripts.conf import DELTALAKE_STORAGE_OPTIONS
        self.duckdb_conn = duckdb.connect()
        self.storage_options = DELTALAKE_STORAGE_OPTIONS

    def write_table(self, df: pl.DataFrame, table_path: str, partition_by: list[str] | str |None = None):
        ''' Writes a Polars DataFrame to a Delta Lake table at the specified path. '''
        try:
            write_deltalake(
                table_path, 
                df.to_arrow(),
                partition_by=partition_by,
                mode="append",
                storage_options=self.storage_options
            )
            print(f"DataFrame written to Delta Lake table at {table_path}.")
        except Exception as e:
            print(f"Failed to write DataFrame to Delta Lake table at {table_path}: {e}")
