from scripts.exploitationImage import init_exploitation_image_pipeline
from pyspark.sql import SparkSession
from scripts.ingest import MinioClient, DeltaLakeClient

def main():

    spark = (
        SparkSession.builder
        .appName("ExploitationImagePipeline")
        .getOrCreate()
    )

    config = spark._jsc.hadoopConfiguration()
    config.set("fs.s3a.endpoint", "http://minio:9000")
    config.set("fs.s3a.path.style.access", "true")
    config.set("fs.s3a.connection.ssl.enabled", "false")
    config.set("fs.s3a.access.key", "minioadmin")
    config.set("fs.s3a.secret.key", "minioadmin")
    config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    init_exploitation_image_pipeline(
        spark=spark,
        trusted_path="s3a://trusted-zone/spoonacular_images/",
        milvus_host="milvus",
        milvus_port=19530,
        collection_name="recipe_images",
        batch_size=64,
    )

    spark.stop()

if __name__ == "__main__":
    main()