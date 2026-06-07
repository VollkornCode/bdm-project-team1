''' File providing methods for ingesting fetched data into the data lake hosted on minio''' 

from deltalake import DeltaTable, write_deltalake
import duckdb
import polars as pl
import boto3
import json
import requests
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
        for bucket in ["raw-data", "deltalake", "trusted-zone", "exploitation-zone"]:
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
        
    def upload_object(self, data, bucket_name: str, key: str) -> bool:
        ''' Uploads a Python object (list/dict) as a JSON file directly to MinIO. '''
        try:
            json_data = json.dumps(data, indent=4)
            
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=json_data,
                ContentType='application/json'
            )
            print(f"Object successfully uploaded to {bucket_name}/{key}")
            return True
        except Exception as e:
            print(f"Failed to upload object: {e}")
            return False
        
    def upload_binary_bytes(self, binary_data: bytes, bucket_name: str, key: str) -> bool:
        '''
        Uploads raw binary bytes (e.g., a processed image) directly 
        to the specified MinIO bucket.
        '''
        try:
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=binary_data,
                ContentType='image/jpeg'
            )
            print(f"Binary bytes successfully uploaded to s3://{bucket_name}/{key}")
            return True
        except Exception as e:
            print(f"Error uploading binary bytes to {bucket_name}/{key}: {e}")
            return False
        
    def upload_binary(self, url: str, bucket_name: str, key: str):
        '''
        Downloads a binary file (e.g., image) from a URL and uploads it 
        directly to the specified MinIO bucket.
        Returns the full S3 path of the uploaded object.
        '''
        try:
            # Download the binary content
            response = requests.get(url, timeout=15)
            response.raise_for_status() # Ensure the request was successful

            # Upload to MinIO
            self.s3_client.put_object(
                Bucket=bucket_name,
                Key=key,
                Body=response.content,
                ContentType=response.headers.get('Content-Type', 'image/jpeg')
            )
            
            s3_path = f"s3://{bucket_name}/{key}"
            print(f"Binary successfully uploaded to {s3_path}")

        except Exception as e:
            print(f"Error uploading binary from {url} to {bucket_name}/{key}: {e}")
        
class DeltaLakeClient:
    ''' Client for interacting with Delta Lake tables. '''
    
    def __init__(self):
        # Ensure it reads the latest storage options (endpoint, keys, etc.)
        from scripts.conf import DELTALAKE_STORAGE_OPTIONS
        self.duckdb_conn = duckdb.connect()
        self.storage_options = DELTALAKE_STORAGE_OPTIONS

    def read_table(self, table_path: str) -> pl.DataFrame:
        try:
            table = DeltaTable(table_path, storage_options=self.storage_options)
            return pl.from_arrow(table.to_pyarrow_table())
        except Exception as e:
            print(f"Error reading Delta Lake at {table_path}: {e}")
            raise

    def write_table(self, data, table_path: str, partition_by: list[str] = None, mode: str = "append"):
        try:
            # 1. Convert to Polars DataFrame
            if isinstance(data, list):
                df = pl.from_dicts(data, strict=False, infer_schema_length=None)
            elif isinstance(data, dict):
                df = pl.from_dicts([data], strict=False, infer_schema_length=None)
            else:
                df = data

            if df.is_empty():
                return

            bad_cols = []

            for col in df.columns:
                dtype = df[col].dtype

                if (
                    dtype == pl.Null
                    or df[col].null_count() == df.height
                    or isinstance(dtype, pl.List)
                    or isinstance(dtype, pl.Struct)
                    or dtype == pl.Object
                ):
                    bad_cols.append(col)

            # 🔧 FIX
            for col in bad_cols:
                dtype = df[col].dtype

                if isinstance(dtype, pl.List) or isinstance(dtype, pl.Struct):
                    # 👉 serializar a JSON string
                    df = df.with_columns(
                        pl.col(col)
                        .map_elements(lambda x: str(x) if x is not None else "NULL")
                        .alias(col)
                    )
                else:
                    # 👉 caso simple
                    df = df.with_columns(
                        pl.col(col)
                        .cast(pl.Utf8, strict=False)
                        .fill_null("NULL")
                        .alias(col)
                    )

            print(f"Fixed schema for problematic columns: {bad_cols}")

            # 4. FINAL SAFETY: ensure no Null dtypes remain
            df = df.select([
                pl.col(c).cast(pl.Utf8) if df[c].dtype == pl.Null else pl.col(c)
                for c in df.columns
            ])

            # 5. Write
            write_deltalake(
                table_path,
                df.to_arrow(),
                storage_options=self.storage_options,
                mode=mode,
                partition_by=partition_by
            )

            print(f"Successfully registered data in Delta Lake at {table_path}")

        except Exception as e:
            print(f"Error writing to Delta Lake at {table_path}: {e}")
