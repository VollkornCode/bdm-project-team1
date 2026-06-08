from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from scripts.imagePreprocessing import preprocess_urls
from scripts.clipModel import CLIPMilvusClient
from scripts.recipeTextModel import RecipeMilvusClient


KAFKA_BROKERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC", "user-image-events")

IMAGE_OUTPUT_PATH     = os.getenv(
    "EXPLOITATION_OUTPUT_PATH",
    "s3a://exploitation-zone/streaming_image_matches/",
)
IMAGE_CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    "s3a://exploitation-zone/checkpoints/streaming_image/",
)

TEXT_OUTPUT_PATH     = os.getenv(
    "TEXT_OUTPUT_PATH",
    "s3a://exploitation-zone/streaming_recipe_matches/",
)
TEXT_CHECKPOINT_PATH = os.getenv(
    "TEXT_CHECKPOINT_PATH",
    "s3a://exploitation-zone/checkpoints/streaming_recipe/",
)

TRIGGER_SECONDS = int(os.getenv("TRIGGER_SECONDS", "30"))

MILVUS_HOST          = os.getenv("MILVUS_HOST", "milvus")
MILVUS_PORT          = int(os.getenv("MILVUS_PORT", "19530"))
IMAGE_COLLECTION     = os.getenv("MILVUS_COLLECTION", "recipe_images")

TEXT_COLLECTION      = os.getenv("MILVUS_TEXT_COLLECTION", "recipe_texts")

DEVICE          = os.getenv("CLIP_DEVICE", "cpu")


EVENT_SCHEMA = StructType([
    StructField("event_type", StringType(), True),   # "image" | "recipe_text"
    StructField("timestamp",  StringType(), True),
    StructField("user_id",    StringType(), True),
    StructField("image_url",  StringType(), True),   # set for "image" events
    StructField("query_text", StringType(), True),   # set for "recipe_text" events
])


class StreamingImageProcessor:

    def __init__(self) -> None:
        self._client = CLIPMilvusClient(
            milvus_host=MILVUS_HOST,
            milvus_port=MILVUS_PORT,
            collection_name=IMAGE_COLLECTION,
            device=DEVICE,
        )

    def process_batch(self, batch_df, batch_id: int) -> None:

        count = batch_df.count()
        if count == 0:
            print(f"[ImageBatch {batch_id}] Empty — skipping.")
            return

        print(f"[ImageBatch {batch_id}] Received {count} events.")

        urls: list[str] = [row["image_url"] for row in batch_df.collect()]

        clean_images = preprocess_urls(urls)

        if not clean_images:
            print(f"[ImageBatch {batch_id}] No valid images after preprocessing — skipping.")
            return

        results = self._client.query(clean_images)

        if not results:
            print(f"[ImageBatch {batch_id}] No Milvus matches — nothing to store.")
            return

        print(f"[ImageBatch {batch_id}] Writing {len(results)} results to '{IMAGE_OUTPUT_PATH}'…")
        spark = SparkSession.getActiveSession()
        (
            spark.createDataFrame(results)
            .write
            .mode("append")
            .json(IMAGE_OUTPUT_PATH)
        )
        print(f"[ImageBatch {batch_id}] Done.")


class StreamingRecipeProcessor:

    def __init__(self) -> None:
        self._client = RecipeMilvusClient(
            milvus_host=MILVUS_HOST,
            milvus_port=MILVUS_PORT,
            collection_name=TEXT_COLLECTION,
            device=DEVICE,
        )

    def process_batch(self, batch_df, batch_id: int) -> None:

        count = batch_df.count()
        if count == 0:
            print(f"[TextBatch {batch_id}] Empty — skipping.")
            return

        print(f"[TextBatch {batch_id}] Received {count} events.")

        queries: list[str] = [row["query_text"] for row in batch_df.collect()]

        results = self._client.query(queries)

        if not results:
            print(f"[TextBatch {batch_id}] No Milvus matches — nothing to store.")
            return

        print(f"[TextBatch {batch_id}] Writing {len(results)} results to '{TEXT_OUTPUT_PATH}'…")
        spark = SparkSession.getActiveSession()
        (
            spark.createDataFrame(results)
            .write
            .mode("append")
            .json(TEXT_OUTPUT_PATH)
        )
        print(f"[TextBatch {batch_id}] Done.")


class StreamingDispatcher:

    def __init__(self) -> None:
        self._image_processor  = StreamingImageProcessor()
        self._recipe_processor = StreamingRecipeProcessor()

    def process_batch(self, batch_df, batch_id: int) -> None:
        total = batch_df.count()
        if total == 0:
            print(f"[Batch {batch_id}] Empty — skipping.")
            return

        print(f"[Batch {batch_id}] Received {total} events — routing by event_type.")

        batch_df.cache()

        image_df = (
            batch_df
            .filter(F.col("event_type") == "image")
            .select(F.col("image_url"))
            .filter(F.col("image_url").isNotNull())
        )
        self._image_processor.process_batch(image_df, batch_id)

        text_df = (
            batch_df
            .filter(F.col("event_type") == "recipe_text")
            .select(F.col("query_text"))
            .filter(F.col("query_text").isNotNull())
        )
        self._recipe_processor.process_batch(text_df, batch_id)

        batch_df.unpersist()


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("StreamingUnifiedPipeline")
        .config("spark.driver.memory", "4g")   # CLIP model requires ~700 MB
        .getOrCreate()
    )

    conf = spark._jsc.hadoopConfiguration()
    conf.set("fs.s3a.endpoint",               "http://minio:9000")
    conf.set("fs.s3a.path.style.access",      "true")
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set("fs.s3a.access.key",             "minioadmin")
    conf.set("fs.s3a.secret.key",             "minioadmin")
    conf.set("fs.s3a.impl",                   "org.apache.hadoop.fs.s3a.S3AFileSystem")

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
        .select(
            F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("data")
        )
        .select(
            F.col("data.event_type").alias("event_type"),
            F.col("data.image_url").alias("image_url"),
            F.col("data.query_text").alias("query_text"),
        )
        .filter(F.col("event_type").isin("image", "recipe_text"))
    )

    dispatcher = StreamingDispatcher()

    query = (
        parsed_stream.writeStream
        .foreachBatch(dispatcher.process_batch)
        .option("checkpointLocation", IMAGE_CHECKPOINT_PATH)
        .trigger(processingTime=f"{TRIGGER_SECONDS} seconds")
        .start()
    )

    print(
        f"--- Unified streaming pipeline started — topic '{KAFKA_TOPIC}', "
        f"micro-batch every {TRIGGER_SECONDS}s ---"
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()