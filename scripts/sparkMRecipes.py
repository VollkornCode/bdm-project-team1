"""
sparkMRecipes.py — Unified Batch Spark entrypoint for the recipe pipeline.

Configures MinIO S3A connections and drives the loading extraction of
usda, spoonocular, and openfoodfacts sources into Milvus.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from scripts.exploitationRecipe import init_exploitation_recipe_pipeline

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("ExploitationMultiSourceRecipePipeline")
        .getOrCreate()
    )

    config = spark._jsc.hadoopConfiguration()
    config.set("fs.s3a.endpoint",               "http://minio:9000")
    config.set("fs.s3a.path.style.access",      "true")
    config.set("fs.s3a.connection.ssl.enabled", "false")
    config.set("fs.s3a.access.key",             "minioadmin")
    config.set("fs.s3a.secret.key",             "minioadmin")
    config.set("fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Target sources definitions
    target_sources = [
        "s3a://trusted-zone/usda/",
        "s3a://trusted-zone/spoonocular/",
        "s3a://trusted-zone/openfoodfacts/"
    ]

    print("=== STARTING ALL SPARK TASKS 2 ===")

    init_exploitation_recipe_pipeline(
        spark=spark,
        trusted_paths=target_sources,
        milvus_host="milvus",
        milvus_port=19530,
        collection_name="recipe_texts",
        batch_size=128,
        device="cpu"
    )

    print("=== ENDING ALL SPARK TASKS ===")
    spark.stop()


if __name__ == "__main__":
    main()