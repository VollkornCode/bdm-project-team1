from pyspark.sql import SparkSession
from scripts.ingest import MinioClient, DeltaLakeClient
import scripts.trustedJSON as trustedJSON


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

    trustedJSON.init_trusted_JSON_pipeline(
        spark=spark,
        minio_client=m_client,
        landing_path="s3a://raw-data/**/*.json",
        trusted_path="JSON_files/"
    )

    spark.stop()


if __name__ == "__main__":
    main()