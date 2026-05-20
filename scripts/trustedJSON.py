from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import (
    col,
    input_file_name,
    substring_index,
    current_timestamp
)
from pyspark.sql.types import StructType
import json

from scripts.ingest import MinioClient


class TrustedJSONClient:

    def __init__(
        self,
        spark: SparkSession,
        landing_path: str,
        trusted_path: str,
        schema: StructType = None
    ):
        self.spark = spark
        self.landing_path = landing_path
        self.trusted_path = trusted_path
        self.schema = schema

        self.df = None

    def load_data_json(self):
        binary_df = (
            self.spark.read
            .format("binaryFile")
            .option("recursiveFileLookup", "true")
            .load(self.landing_path)
            .filter(col("path").endswith(".json"))
        )

        rows = binary_df.collect()
        parsed_records = []

        for row in rows:
            try:
                content = row["content"].decode("utf-8")
                data = json.loads(content)
                filename = row["path"].split("/")[-1]

                if isinstance(data, list):
                    for item in data:
                        item["filename"] = filename
                        parsed_records.append(item)
                elif isinstance(data, dict):
                    if "products" in data:
                        for item in data["products"]:
                            item["filename"] = filename
                            parsed_records.append(item)
                    elif "recipes" in data:
                        for item in data["recipes"]:
                            item["filename"] = filename
                            parsed_records.append(item)
                    else:
                        data["filename"] = filename
                        parsed_records.append(data)

            except Exception as e:
                print(f"Error parsing {row['path']}: {e}")

        if not parsed_records:
            raise RuntimeError("No JSON records could be parsed.")

        json_strings = [json.dumps(record, default=str) for record in parsed_records]
        rdd = self.spark.sparkContext.parallelize(json_strings)
        self.df = self.spark.read.json(rdd)

        print(f"Loaded {self.df.count()} records")
        return self.df

    def clean_data_json(self):
        self.df = (
            self.df
            .dropDuplicates()
            .dropna(how="all")
            .withColumn("processed_at", current_timestamp())
        )

        print(f"Valid records after cleaning: {self.df.count()}")
        return self.df

    def store_data_json(self, minio_client: MinioClient):
        minio_client.create_buckets()

        rows = self.df.collect()

        if not rows:
            print("No JSON data to upload.")
            return

        uploaded_count = 0

        for i, row in enumerate(rows): 
            filename = row["filename"]
            stem = filename.rsplit(".", 1)[0]  # quita extensión

            row_dict = row.asDict()
            row_dict.pop("path", None)

            json_bytes = json.dumps(
                row_dict,
                default=str,
                indent=2
            ).encode("utf-8")

            # 👇 nombre único por registro: usda_20260519_204602_0.json
            s3_key = f"{self.trusted_path}{stem}_{i}.json"

            success = minio_client.upload_binary_bytes(
                binary_data=json_bytes,
                bucket_name="trusted-zone",
                key=s3_key
            )

            if success:
                uploaded_count += 1

        print(f"Successfully processed and stored {uploaded_count} JSON files.")


def init_trusted_JSON_pipeline(
    spark: SparkSession,
    minio_client: MinioClient,
    landing_path: str,
    trusted_path: str,
    schema: StructType = None
):
    """
    Orchestrates the ETL process for JSON:
    1. Load
    2. Clean
    3. Store
    """

    print("--- Starting Trusted JSON Pipeline ---")

    try:

        client = TrustedJSONClient(
            spark=spark,
            landing_path=landing_path,
            trusted_path=trusted_path,
            schema=schema
        )

        try:
            minio_client.create_buckets()
            print(
                "Bucket 'trusted-zone' verified/created."
            )
        except Exception:
            pass

        client.load_data_json()
        client.clean_data_json()
        client.store_data_json(minio_client)

    except Exception as e:
        print(
            f"Critical error during JSON "
            f"Trusted pipeline execution: {e}"
        )

    finally:
        spark.stop()
        print(
            "--- Trusted JSON Pipeline Completed ---"
        )


if __name__ == "__main__":

    init_trusted_json_pipeline(
        spark=spark,
        minio_client=minio_client,
        landing_path="s3a://raw-data/spoonacular/json/",
        trusted_path="spoonacular_json/"
    )