"""
sparkStreamingRecipes.py — Spark Structured Streaming entrypoint for text queries.

Reads string content raw events from a real-time user-request Kafka topic,
vectorises the inputs using sentence embeddings, queries Milvus for the closest Top-1 
recipe matches, and writes outcomes as structured JSON outputs to the exploitation zone.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from scripts.recipeTextModel import RecipeMilvusClient

# ── Config ─────────────────────────────────────────────────────────────────────

KAFKA_BROKERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "user-recipe-text-events")
OUTPUT_PATH     = os.getenv(
    "EXPLOITATION_OUTPUT_PATH",
    "s3a://exploitation-zone/streaming_recipe_matches/",
)
CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    "s3a://exploitation-zone/checkpoints/streaming_recipes/",
)
TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "30"))
MILVUS_HOST     = os.getenv("MILVUS_HOST", "milvus")
MILVUS_PORT     = int(os.getenv("MILVUS_PORT", "19530"))
COLLECTION_NAME = os.getenv("MILVUS_COLLECTION", "recipe_texts")
DEVICE          = os.getenv("TEXT_DEVICE", "cpu")

# ── Kafka Schema ───────────────────────────────────────────────────────────────

EVENT_SCHEMA = StructType([
    StructField("timestamp",    StringType(), True),
    StructField("user_id",      StringType(), True),
    StructField("search_input", StringType(), True),
])

# ── Micro-batch handler ────────────────────────────────────────────────────────

class StreamingRecipeProcessor:
    """Orchestrates stream micro-batches by interacting with the client engine."""

    def __init__(self) -> None:
        self._client = RecipeMilvusClient(
            milvus_host=MILVUS_HOST,
            milvus_port=MILVUS_PORT,
            collection_name=COLLECTION_NAME,
            device=DEVICE,
        )

    def process_batch(self, batch_df, batch_id: int) -> None:
        count = batch_df.count()
        if count == 0:
            print(f"[Batch {batch_id}] Empty payload — skipping.")
            return

        print(f"[Batch {batch_id}] Received {count} real-time search queries.")

        # Collect raw query search strings to driver
        queries: list[str] = [str(row["search_input"]).strip() for row in batch_df.collect()]

        # Generate vectors and hit common Milvus index collection
        results = self._client.query(queries)

        if not results:
            print(f"[Batch {batch_id}] Zero cross-reference targets hit in Milvus.")
            return

        # Write matches into target exploitation layer sink destination
        print(f"[Batch {batch_id}] Writing {len(results)} outputs to '{OUTPUT_PATH}'…")
        spark = SparkSession.getActiveSession()
        (
            spark.createDataFrame(results)
            .write
            .mode("append")
            .json(OUTPUT_PATH)
        )
        print(f"[Batch {batch_id}] Execution block microbatch cycle done.")


# ── Streaming Job Main Entrypoint ──────────────────────────────────────────────

def main() -> None:
    spark = (
        SparkSession.builder
        .appName("StreamingRecipeTextPipeline")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    # Hadoop S3A MinIO Storage Target settings
    conf = spark._jsc.hadoopConfiguration()
    conf.set("fs.s3a.endpoint",               "http://minio:9000")
    conf.set("fs.s3a.path.style.access",      "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set("fs.s3a.access.key",             "minioadmin")
    conf.set("fs.s3a.secret.key",             "minioadmin")
    conf.set("fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Read from Kafka streaming context
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data"))
        .select(F.col("data.search_input").alias("search_input"))
        .filter(F.col("search_input").isNotNull())
    )

    processor = StreamingRecipeProcessor()

    query = (
        parsed_stream.writeStream
        .foreachBatch(processor.process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    print(f"--- Real-time Text pipeline streaming from Kafka topic '{KAFKA_TOPIC}' ---")
    query.awaitTermination()


if __name__ == "__main__":
    main()