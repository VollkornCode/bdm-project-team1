from pyspark.sql import SparkSession
from scripts.ingest import MinioClient, DeltaLakeClient
import scripts.trustedParquet as trustedParquet


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

    d_client = DeltaLakeClient()

    print("=== SPOONACULAR ===")
    trustedParquet.init_trusted_parquet_pipeline(
        spark=spark,
        delta_client=d_client,
        landing_path="s3a://deltalake/spoonocular/", 
        trusted_path="s3a://trusted-zone/spoonocular/"
    )

    print("=== USDA ===")
    trustedParquet.init_trusted_parquet_pipeline(
        spark=spark,
        delta_client=d_client,
        landing_path="s3a://deltalake/usda/", 
        trusted_path="s3a://trusted-zone/usda/"
    )

    print("=== ENDING ALL SPARK TASKS ===")
    spark.stop()


if __name__ == "__main__":
    main()