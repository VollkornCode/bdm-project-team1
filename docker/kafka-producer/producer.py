import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

KAFKA_BROKERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "user-image-events")
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "1"))
INTERVAL_SECONDS = float(os.getenv("INTERVAL_SECONDS", "60"))
IMAGE_EVENT_RATIO = float(os.getenv("IMAGE_EVENT_RATIO", "0.5"))

IMAGE_URL_POOL: list[str] = [
    "https://img.spoonacular.com/recipes/716429-312x231.jpg",
    "https://img.spoonacular.com/recipes/715538-312x231.jpg",
    "https://img.spoonacular.com/recipes/782601-312x231.jpg",
    "https://img.spoonacular.com/recipes/715769-312x231.jpg",
    "https://img.spoonacular.com/recipes/644387-312x231.jpg",
    "https://img.spoonacular.com/recipes/660306-312x231.jpg",
    "https://img.spoonacular.com/recipes/511728-312x231.jpg",
    "https://img.spoonacular.com/recipes/637876-312x231.jpg",
    "https://img.spoonacular.com/recipes/715594-312x231.jpg",
]

QUERY_TEXT_POOL: list[str] = [
    "quick pasta with tomato sauce",
    "healthy chicken salad low calorie",
    "vegan chocolate cake",
    "gluten free banana bread",
    "spicy thai green curry",
    "creamy mushroom risotto",
    "easy breakfast smoothie",
    "classic beef burger homemade",
    "lemon garlic shrimp",
    "vegetarian tacos with black beans",
]


def create_topic_if_not_exists() -> None:
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,
            client_id="admin-creator",
        )
        admin.create_topics(
            new_topics=[NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)],
            validate_only=False,
        )
        print(f"Topic '{KAFKA_TOPIC}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{KAFKA_TOPIC}' already exists — skipping creation.")
    except Exception as exc:
        print(f"Could not create topic: {exc}")
    finally:
        admin.close()


def _base_fields() -> dict:
    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "user_id":   random.randint(1000, 9999),
    }


def make_image_record() -> dict:
    return {
        **_base_fields(),
        "event_type": "image",
        "image_url":  random.choice(IMAGE_URL_POOL),
        "query_text": None,
    }


def make_text_record() -> dict:
    return {
        **_base_fields(),
        "event_type": "recipe_text",
        "image_url":  None,
        "query_text": random.choice(QUERY_TEXT_POOL),
    }


def make_record() -> dict:
    if random.random() < IMAGE_EVENT_RATIO:
        return make_image_record()
    return make_text_record()


def connect(retries: int = 20, delay: float = 3.0) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Connected to Kafka at {KAFKA_BROKERS}")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready (attempt {attempt}/{retries}), retrying in {delay}s…")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts.")


def main() -> None:
    create_topic_if_not_exists()
    producer = connect()
    batch_num = 0

    while True:
        batch_num += 1
        records = [make_record() for _ in range(BATCH_SIZE)]

        for rec in records:
            try:
                # Synchronous send — waits for broker acknowledgement.
                producer.send(KAFKA_TOPIC, value=rec).get(timeout=10)
            except Exception as exc:
                print(f"Send error: {exc}")

        producer.flush()
        image_count = sum(1 for r in records if r["event_type"] == "image")
        text_count  = BATCH_SIZE - image_count
        print(
            f"Batch {batch_num} | published {BATCH_SIZE} records "
            f"to topic '{KAFKA_TOPIC}' "
            f"({image_count} image, {text_count} recipe_text)"
        )
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()