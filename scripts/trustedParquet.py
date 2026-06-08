from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
import json
import io

from scripts.ingest import MinioClient, DeltaLakeClient


def _flatten_struct_columns(df: DataFrame) -> DataFrame:
    max_depth = 7
    depth = 0
    
    while depth < max_depth:
        struct_cols = [
            field.name
            for field in df.schema.fields
            if isinstance(field.dataType, StructType)
        ]

        if not struct_cols:
            break

        print(f"    [Flatten] Nivel {depth+1}: Aplanando estructuras -> {struct_cols}")
        
        expand_exprs = []
        for field in df.schema.fields:
            if isinstance(field.dataType, StructType):
                for subfield in field.dataType.fields:
                    alias = f"{field.name}__{subfield.name}"
                    expand_exprs.append(F.col(f"{field.name}.{subfield.name}").alias(alias))
            else:
                expand_exprs.append(F.col(field.name))
        
        df = df.select(*expand_exprs)
        depth += 1
        
    if depth == max_depth:
        print("    [Flatten] ⚠️ Max depth reached — stopped flattening to avoid infinite loop.")
        
    return df


REGISTRY_BUCKET = "trusted-zone"
REGISTRY_PREFIX = "_processed_registry"


def _registry_key(folder: str) -> str:
    return f"{REGISTRY_PREFIX}/{folder}.json"


def _load_registry(minio_client: MinioClient, folder: str) -> set[str]:
    key = _registry_key(folder)
    try:
        response = minio_client.s3_client.get_object(Bucket=REGISTRY_BUCKET, Key=key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        processed = set(data.get("processed_files", []))
        print(f"[Registry] Loaded {len(processed)} already-processed files for '{folder}'.")
        return processed
    except minio_client.s3_client.exceptions.NoSuchKey:
        print(f"[Registry] No registry found for '{folder}' — treating all files as new.")
        return set()
    except Exception as exc:
        print(f"[Registry] Could not load registry for '{folder}': {exc} — treating all files as new.")
        return set()


def _save_registry(minio_client: MinioClient, folder: str, processed_files: set[str]) -> None:
    key = _registry_key(folder)
    data = json.dumps({"processed_files": sorted(processed_files)}, indent=2)
    minio_client.s3_client.put_object(
        Bucket=REGISTRY_BUCKET,
        Key=key,
        Body=data.encode("utf-8"),
        ContentType="application/json",
    )
    print(f"[Registry] Saved registry for '{folder}' ({len(processed_files)} files total).")


def _list_parquet_files(minio_client: MinioClient, folder: str) -> list[str]:
    paginator = minio_client.s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket="deltalake", Prefix=f"{folder}/")

    files = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                files.append(key)   # store full key for path building
    return files


class TrustedParquetClient:

    LANDING_BUCKET_PREFIX = "s3a://deltalake/"

    def __init__(self, spark: SparkSession, landing_path: str, trusted_path: str):
        self.spark        = spark
        self.landing_path = landing_path
        self.trusted_path = trusted_path

        self.df: DataFrame | None = None
        self._partitioned_dfs: dict[str, DataFrame] = {}

    def load_data(self, new_files: list[str] | None = None) -> DataFrame:
        if new_files:
            paths = [f"s3a://deltalake/{key}" for key in new_files]
            print(f"[Load] Reading {len(paths)} new Parquet file(s)...")
            self.df = (
                self.spark.read
                .parquet(*paths)
                .withColumn("_source_path", F.input_file_name())
            )
        else:
            print(f"[Load] Reading all Parquet files from: {self.landing_path}")
            self.df = (
                self.spark.read
                .option("pathGlobFilter", "*.parquet")
                .option("recursiveFileLookup", "true")
                .parquet(self.landing_path)
                .withColumn("_source_path", F.input_file_name())
            )

        row_count = self.df.count()
        print(f"[Load] Loaded {row_count:,} rows.")
        return self.df

    def clean_data(self) -> dict[str, DataFrame]:
        if self.df is None:
            raise ValueError("[Clean] No data loaded. Call load_data() first.")

        print("--- Cleaning Data (Flatten & Filter) ---")
        df = self.df

        df = df.withColumn(
            "_clean_path",
            F.regexp_replace(F.col("_source_path"), f"^{self.LANDING_BUCKET_PREFIX}", "")
        )
        df = df.withColumn(
            "_source_folder",
            F.when(
                F.col("_clean_path").contains("/"),
                F.regexp_extract(F.col("_clean_path"), r"^([^/]+)", 1)
            ).otherwise(F.lit("recipes"))
        ).drop("_clean_path")

        print("[Clean] Step 2/4 – Flattening nested struct columns...")
        df = _flatten_struct_columns(df)

        print("[Clean] Step 3/4 – Dropping rows with all null values...")
        data_cols = [c for c in df.columns if c not in ["_source_path", "_source_folder"]]
        df = df.dropna(how="all", subset=data_cols)

        print("[Clean] Step 4/4 – Dropping exact duplicate rows...")
        df = df.dropDuplicates(subset=data_cols)

        after_dedup = df.count()

        print("[Clean] Partitioning DataFrames by source folder...")
        source_folders = [
            r[0] for r in df.select("_source_folder").distinct().collect()
            if r[0] is not None
        ]
        print(f"    Detected source folders: {source_folders}")

        self._partitioned_dfs = {}
        for folder in source_folders:
            sub_df = (
                df.filter(F.col("_source_folder") == folder)
                  .drop("_source_path", "_source_folder")
            )
            self._partitioned_dfs[folder] = sub_df

        print(f"[Clean] Finished. {after_dedup:,} rows across {len(source_folders)} partition(s).")
        return self._partitioned_dfs

    def store_data(self, delta_client: DeltaLakeClient) -> None:
        if not self._partitioned_dfs:
            raise RuntimeError("No partitioned data. Call clean_data() first.")

        print(f"[Store] Writing {len(self._partitioned_dfs)} partition(s) to Trusted Zone...")

        for source_folder, sub_df in self._partitioned_dfs.items():
            destination_path = f"{self.trusted_path.rstrip('/')}/{source_folder}"
            row_count        = sub_df.count()
            print(f"[Store] Writing '{source_folder}' ({row_count:,} rows) → {destination_path}")

            try:
                rows_as_dicts = [row.asDict(recursive=True) for row in sub_df.collect()]
                delta_client.write_table(
                    data=rows_as_dicts,
                    table_path=destination_path,
                )
                print(f"    ✓ '{source_folder}' stored successfully.")
            except Exception as e:
                print(f"    ✗ Error storing '{source_folder}': {e}")

        print(f"[Store] Done.")


def init_trusted_parquet_pipeline(
    spark: SparkSession,
    delta_client: DeltaLakeClient,
    minio_client: MinioClient,
    landing_path: str,
    trusted_path: str,
) -> None:
    
    print("=" * 60)
    print("--- Starting Trusted Parquet Pipeline ---")
    print(f"    Landing path : {landing_path}")
    print(f"    Trusted path : {trusted_path}")
    print("=" * 60)

    folder = landing_path.rstrip("/").split("/")[-1]

    try:
        all_files = _list_parquet_files(minio_client, folder)
        print(f"[Incremental] Found {len(all_files)} total .parquet file(s) in '{folder}'.")

        processed = _load_registry(minio_client, folder)

        new_files = [f for f in all_files if f not in processed]
        print(f"[Incremental] {len(new_files)} new file(s) to process "
              f"({len(all_files) - len(new_files)} already processed — skipping).")

        if not new_files:
            print("[Incremental] Nothing to do — all files already processed.")
            return

        client = TrustedParquetClient(
            spark=spark,
            landing_path=landing_path,
            trusted_path=trusted_path,
        )

        client.load_data(new_files=new_files)
        client.clean_data()
        client.store_data(delta_client)

        updated = processed | set(new_files)
        _save_registry(minio_client, folder, updated)

    except Exception as e:
        print(f"[Pipeline] Critical error: {e}")
        raise

    finally:
        print("=" * 60)
        print("--- Trusted Parquet Pipeline Completed ---")
        print("=" * 60)