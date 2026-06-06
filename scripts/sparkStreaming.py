"""
sparkStreaming.py — Spark Structured Streaming entrypoint for real-time
                    image similarity.

Orchestration only: this script reads from Kafka, extracts URLs, and
delegates all work to the two domain scripts:

  imagePreprocessing.py  — download + validate + resize images
  clipModel.py           — CLIP embedding + Milvus Top-1 search

Data flow per micro-batch
─────────────────────────
  Kafka  →  parse JSON  →  [image_url, ...]
         →  imagePreprocessing.preprocess_urls()
         →  clipModel.CLIPMilvusClient.query()
         →  write {image_url, filename} JSON to MinIO

Usage
─────
  spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
      sparkStreaming.py
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from scripts.imagePreprocessing import preprocess_urls
from scripts.clipModel import CLIPMilvusClient

# ── Config (overridable via environment variables) ─────────────────────────────

KAFKA_BROKERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "user-image-events")
OUTPUT_PATH     = os.getenv(
    "EXPLOITATION_OUTPUT_PATH",
    "s3a://exploitation-zone/streaming_image_matches/",
)
CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    "s3a://exploitation-zone/checkpoints/streaming_image/",
)
TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "30"))
MILVUS_HOST     = os.getenv("MILVUS_HOST", "milvus")
MILVUS_PORT     = int(os.getenv("MILVUS_PORT", "19530"))
COLLECTION_NAME = os.getenv("MILVUS_COLLECTION", "recipe_images")
DEVICE          = os.getenv("CLIP_DEVICE", "cpu")

# ── Kafka JSON schema ──────────────────────────────────────────────────────────

EVENT_SCHEMA = StructType([
    StructField("timestamp", StringType(), True),
    StructField("user_id",   StringType(), True),
    StructField("image_url", StringType(), True),
])

# ── Micro-batch handler ────────────────────────────────────────────────────────

class StreamingImageProcessor:
    """
    Coordinates the two domain scripts for each Spark micro-batch.
    Holds the CLIPMilvusClient as state so the model is loaded only once.
    """

    def __init__(self) -> None:
        # Instantiated once; CLIP weights and Milvus connection are lazy-loaded
        # inside CLIPMilvusClient on the first real micro-batch.
        self._client = CLIPMilvusClient(
            milvus_host=MILVUS_HOST,
            milvus_port=MILVUS_PORT,
            collection_name=COLLECTION_NAME,
            device=DEVICE,
        )

    def process_batch(self, batch_df, batch_id: int) -> None:
        """
        Entry point called by Spark for every micro-batch.

        Parameters
        ----------
        batch_df : pyspark.sql.DataFrame
            Schema: (image_url STRING)
        batch_id : int
            Monotonically increasing micro-batch sequence number.
        """
        count = batch_df.count()
        if count == 0:
            print(f"[Batch {batch_id}] Empty — skipping.")
            return

        print(f"[Batch {batch_id}] Received {count} events.")

        # ── Step 1: collect URLs to driver ─────────────────────────────────────
        urls: list[str] = [row["image_url"] for row in batch_df.collect()]

        # ── Step 2: download + preprocess (imagePreprocessing.py) ──────────────
        clean_images = preprocess_urls(urls)

        if not clean_images:
            print(f"[Batch {batch_id}] No valid images after preprocessing — skipping.")
            return

        # ── Step 3: embed + Milvus search (clipModel.py) ───────────────────────
        results = self._client.query(clean_images)

        if not results:
            print(f"[Batch {batch_id}] No Milvus matches — nothing to store.")
            return

        # ── Step 4: write results to Exploitation Zone (MinIO) ─────────────────
        print(f"[Batch {batch_id}] Writing {len(results)} results to '{OUTPUT_PATH}'…")
        spark = SparkSession.getActiveSession()
        (
            spark.createDataFrame(results)
            .write
            .mode("append")
            .json(OUTPUT_PATH)
        )
        print(f"[Batch {batch_id}] Done.")

# ── Streaming job ──────────────────────────────────────────────────────────────

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("StreamingImagePipeline")
        .config("spark.driver.memory", "4g")   # CLIP model requires ~700 MB
        .getOrCreate()
    )

    # ── S3A / MinIO settings ───────────────────────────────────────────────────
    conf = spark._jsc.hadoopConfiguration()
    conf.set("fs.s3a.endpoint",               "http://minio:9000")
    conf.set("fs.s3a.path.style.access",      "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set("fs.s3a.access.key",             "minioadmin")
    conf.set("fs.s3a.secret.key",             "minioadmin")
    conf.set("fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # ── Kafka source ───────────────────────────────────────────────────────────
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON and extract image_url ───────────────────────────────────────
    parsed_stream = (
        raw_stream
        .select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data")
        )
        .select(F.col("data.image_url").alias("image_url"))
        .filter(F.col("image_url").isNotNull())
    )

    # ── Start streaming query ──────────────────────────────────────────────────
    processor = StreamingImageProcessor()

    query = (
        parsed_stream.writeStream
        .foreachBatch(processor.process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    print(
        f"--- Streaming pipeline started — topic '{KAFKA_TOPIC}', "
        f"micro-batch every {TRIGGER_SECONDS}s ---"
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()