"""
producer.py — Kafka producer that simulates real-time user image events.

Each message contains:
  - timestamp : ISO-8601 UTC string
  - user_id   : random integer representing a user
  - image_url : URL pointing to a publicly accessible recipe image

The image URLs are sampled from a small pool of Spoonacular-style
public image endpoints.  Replace or extend IMAGE_URL_POOL with real
URLs from your environment or a test HTTP server.
"""

import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

# ── Environment / config ───────────────────────────────────────────────────────

KAFKA_BROKERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC      = os.getenv("KAFKA_TOPIC", "user-image-events")
BATCH_SIZE       = int(os.getenv("BATCH_SIZE", "1"))
INTERVAL_SECONDS = float(os.getenv("INTERVAL_SECONDS", "60"))

# Pool of sample public recipe image URLs (Spoonacular CDN pattern).
# Extend this list with real URLs from your API quota or test server.
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

# ── Topic management ───────────────────────────────────────────────────────────

def create_topic_if_not_exists() -> None:
    """Create the Kafka topic when it does not already exist."""
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

# ── Event factory ──────────────────────────────────────────────────────────────

def make_record() -> dict:
    """
    Build a single user-image event payload.

    Returns
    -------
    dict
        {
            "timestamp" : "2026-06-05T11:45:00.123456+00:00",
            "user_id"   : <int 1000-9999>,
            "image_url" : "<URL from IMAGE_URL_POOL>"
        }
    """
    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "user_id":   random.randint(1000, 9999),
        "image_url": random.choice(IMAGE_URL_POOL),
    }

# ── Kafka connection with retry ────────────────────────────────────────────────

def connect(retries: int = 20, delay: float = 3.0) -> KafkaProducer:
    """Connect to Kafka, retrying up to *retries* times with *delay* seconds gap."""
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

# ── Main loop ──────────────────────────────────────────────────────────────────

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
        print(
            f"Batch {batch_num} | published {BATCH_SIZE} records "
            f"to topic '{KAFKA_TOPIC}'"
        )
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()