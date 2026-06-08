from pyspark.sql import SparkSession
from scripts.ingest import MinioClient, DeltaLakeClient
import scripts.trustedParquet as trustedParquet


def _list_source_folders(minio_client: MinioClient, bucket: str) -> list[str]:
    paginator = minio_client.s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Delimiter="/")

    folders = []
    for page in pages:
        for prefix in page.get("CommonPrefixes", []):
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

    source_folders = _list_source_folders(m_client, bucket="deltalake")
    print(f"=== Discovered source folders: {source_folders} ===")

    for folder in source_folders:
        print(f"=== PROCESSING: {folder} ===")
        trustedParquet.init_trusted_parquet_pipeline(
            spark=spark,
            delta_client=d_client,
            minio_client=m_client,
            landing_path=f"s3a://deltalake/{folder}/",
            trusted_path="s3a://trusted-zone/"
        )

    spark.stop()


if __name__ == "__main__":
    main()