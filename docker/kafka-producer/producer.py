import random
import time
import os
import json
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS","127.0.0.1:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC","user-info-raw")
BATCH_SIZE = int(os.getenv("BATCH_SIZE","10"))
INTERVAL_SECONDS = float(os.getenv("INTERVAL_SECONDS", "10"))

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

def create_topic_if_not_exists():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKERS,
            client_id='admin-creator'
        )
        
        topic_list = [NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{KAFKA_TOPIC}' creado exitosamente.")
        
    except TopicAlreadyExistsError:
        print(f"El topic '{KAFKA_TOPIC}' ya existe, saltando creación.")
    except Exception as e:
        print(f"No se pudo crear el topic: {e}")
    finally:
        admin_client.close()

# user: timestamp, application used
def make_record() -> dict:
    applications = ['recommender', 'price_predictor', 'nutrient_detector']
    application_used = random.choice(applications)

    return {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "application": application_used
    }
    
def connect(retries: int = 20, delay: float = 3.0) -> KafkaProducer:
    for attempt in range(1, retries+1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"Connected to Kafka at {KAFKA_BROKERS}")
            return producer
        except NoBrokersAvailable:
            print(f"Kafka not ready yet (attempt {attempt}/{retries}), retrying in {delay}s…")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka after {retries} attempts")

def main() -> None:
    create_topic_if_not_exists()
    producer = connect()
    batch_num = 0
    
    while True:
        batch_num += 1
        records = [make_record() for _ in range(BATCH_SIZE)]
        
        for rec in records:
            try:
                # El .get(timeout=10) obliga al script a esperar la respuesta REAL de Kafka
                producer.send('user-info-raw', value=rec).get(timeout=10)
            except Exception as e:
                print(f"ERROR REAL AL ENVIAR: {e}")
        producer.flush()
        
        print(f"Batch {batch_num} | published {BATCH_SIZE} records ")
        time.sleep(INTERVAL_SECONDS)
        
if __name__ == "__main__":
    main()
        