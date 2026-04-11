import json
import time
import os
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from scripts.ingest import MinioClient, DeltaLakeClient

from scripts.conf import (
    KAFKA_BOOTSTRAP_SERVERS, 
    KAFKA_TOPIC, KAFKA_GROUP, 
    CONSUMER_TIMEOUT_MS, 
)
from scripts.ingest import MinioClient

class KafkaConsumerClient:
    ''' Client to consume messages from Kafka and persist them into the Data Lakehouse. '''

    def __init__(self):
        # Initialize connection parameters from conf.py
        self.bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS
        self.topic = KAFKA_TOPIC
        self.group_id = KAFKA_GROUP
        self.timeout_ms = CONSUMER_TIMEOUT_MS
        
        # Establish connection to Kafka
        self.consumer = self._connect()

    def _connect(self, retries=10, delay=5):
        for i in range(1, retries + 1):
            try:
                consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap_servers,
                    group_id=self.group_id,
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    consumer_timeout_ms=self.timeout_ms
                )
                print(f"Successfully connected to Kafka at {self.bootstrap_servers}")
                return consumer
            except NoBrokersAvailable:
                print(f"Kafka broker not available (attempt {i}/{retries}). Retrying in {delay}s...")
                time.sleep(delay)
        raise RuntimeError(f"Could not connect to Kafka after {retries} attempts.")

def init_fetch(minio_client: MinioClient):
    
    '''
    Main execution flow:
    1. Fetch messages from Kafka topic.
    2. Save raw JSON data to MinIO.
    '''

    # Initialize Clients
    kafka_client = KafkaConsumerClient()
    
    # Ensure buckets exists
    minio_client.create_buckets()
    
    messages = []
    try:
        # 1. Consume available messages until timeout is reached
        for msg in kafka_client.consumer:
            messages.append(msg.value)
        
        if not messages:
            print(f"No new messages found in topic: {kafka_client.topic}")
            return

        print(f"Fetched {len(messages)} messages from Kafka.")

        # 2. Define naming convention and paths
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        file_path = f"kafka_ingestion/{kafka_client.topic}/{timestamp}_batch.json"
        
        # 3. Upload the file to MinIO "raw-data" bucket.
        minio_client.upload_object(messages, "raw-data", file_path)
        print(f"Raw data successfully saved to MinIO: raw-data/{file_path}")

    except Exception as e:
        print(f"Error during Kafka ingestion process: {e}")
        raise
    finally:
        # Ensure the consumer is closed to release resources
        kafka_client.consumer.close()
        print("Kafka consumer connection closed.")