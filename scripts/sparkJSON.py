from pyspark.sql import SparkSession
from scripts.ingest import MinioClient, DeltaLakeClient
import scripts.trustedParquet as trustedParquet


def _list_source_folders(minio_client: MinioClient, bucket: str) -> list[str]:
    """
    List immediate subfolders inside *bucket* using the MinIO s3_client (boto3).
    Returns folder names without trailing slash, e.g. ['spoonocular', 'usda'].

    Uses the boto3 paginator with Delimiter='/' so S3 returns CommonPrefixes
    (virtual folders) instead of individual objects — equivalent to 'ls' on
    the bucket root without recursing into subfolders.
    """
    paginator = minio_client.s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Delimiter="/")

    folders = []
    for page in pages:
        for prefix in page.get("CommonPrefixes", []):
            # Each CommonPrefix looks like "spoonocular/" — strip the slash.
            folder = prefix["Prefix"].rstrip("/")
            folders.append(folder)

    return folders


def main():

    spark = (
        SparkSession.builder
        .appName("TrustedJSONPipeline")
        .getOrCreate()
    )

    config = spark._jsc.hadoopConfiguration()

    config.set("fs.s3a.endpoint", "http://minio:9000")
    config.set("fs.s3a.path.style.access", "true")
    config.set("fs.s3a.connection.ssl.enabled", "false")
    config.set("fs.s3a.access.key", "minioadmin")
    config.set("fs.s3a.secret.key", "minioadmin")
    config.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    m_client = MinioClient()
    d_client = DeltaLakeClient()

    # Discover subfolders dynamically from MinIO instead of relying on S3A
    # root listing, which can silently skip prefixes in some MinIO versions.
    source_folders = _list_source_folders(m_client, bucket="deltalake")
    print(f"=== Discovered source folders: {source_folders} ===")

    for folder in source_folders:
        print(f"=== PROCESSING: {folder} ===")
        trustedParquet.init_trusted_parquet_pipeline(
            spark=spark,
            delta_client=d_client,
            landing_path=f"s3a://deltalake/{folder}/",
            trusted_path=f"s3a://trusted-zone/"
        )

    print("=== ENDING ALL SPARK TASKS ===")
    spark.stop()


if __name__ == "__main__":
    main()