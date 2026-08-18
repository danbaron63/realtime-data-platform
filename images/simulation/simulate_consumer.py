import logging
import os
import queue
import signal
import threading
import time

import httpx
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from prometheus_client import Counter, Gauge, start_http_server

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
stop_event = threading.Event()


def main():
    start_http_server(8000)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    event_queue = queue.Queue(maxsize=1000)
    event_queue_size_gauge = Gauge(
        "simulate_consumer_event_queue_size_gauge",
        "approximate size of the internal event queue of simulate_consumer",
    )
    feature_store_responses_counter = Counter(
        "simulate_consumer_feature_store_responses_counter",
        "counter of the responses from the feature store",
        labelnames=["feature", "http_status_code"],
    )

    workers = [
        threading.Thread(
            target=worker, args=(i, event_queue, feature_store_responses_counter)
        )
        for i in range(20)
    ]

    for thread in workers:
        thread.start()

    kafka_thread = threading.Thread(
        target=kafka_consumer, args=(event_queue, event_queue_size_gauge)
    )
    kafka_thread.start()

    kafka_thread.join()
    for thread in workers:
        thread.join()


def worker(
    worker_id: int, event_queue: queue.Queue, feature_store_responses_counter: Counter
):
    http_client = httpx.Client(timeout=5)

    try:
        while not stop_event.is_set():
            try:
                record = event_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                time.sleep(0.15)

                account_id = record["account_id"]
                customer_id = record["customer_id"]
                get_payment_features(
                    account_id, http_client, feature_store_responses_counter
                )
                get_customer_payment_features(
                    customer_id, http_client, feature_store_responses_counter
                )
            finally:
                event_queue.task_done()
    finally:
        http_client.close()
        logger.info(f"worker {worker_id} stopped")


def kafka_consumer(event_queue: queue.Queue, event_queue_size_gauge: Gauge):
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "simulator",
            "auto.offset.reset": "latest",
        }
    )
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserialiser = AvroDeserializer(schema_registry_client)
    consumer.subscribe(["raw.payment_authorised"])

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                logger.error(f"Error fetching record: {msg.error()}")
                continue

            record = avro_deserialiser(
                msg.value(), SerializationContext(msg.topic(), MessageField.VALUE)
            )

            account_id = record["account_id"]
            customer_id = record["customer_id"]

            event_queue.put({"account_id": account_id, "customer_id": customer_id})

            event_queue_size_gauge.set(event_queue.qsize())
    finally:
        consumer.close()
        logger.info("kafka consumer stopped")


def get_payment_features(
    account_id: str, http_client: httpx.Client, feature_store_responses_counter: Counter
):
    response = http_client.post(
        f"{FEATURE_STORE_URL}/features/payment_6h",
        headers={
            "Content-Type": "application/json",
        },
        json={"entity_keys": {"account_id": account_id}},
    )
    feature_store_responses_counter.labels(
        feature="payment_6h", http_status_code=response.status_code
    ).inc()
    logger.info(
        f"[payment_6h] {response.status_code} response for {account_id=}: {response.json()}"
    )


def get_customer_payment_features(
    customer_id: str,
    http_client: httpx.Client,
    feature_store_responses_counter: Counter,
):
    response = http_client.post(
        f"{FEATURE_STORE_URL}/features/customer_payment_6h",
        headers={
            "Content-Type": "application/json",
        },
        json={"entity_keys": {"customer_id": customer_id}},
    )
    feature_store_responses_counter.labels(
        feature="customer_payment_6h", http_status_code=response.status_code
    ).inc()
    logger.info(
        f"[customer_payment_6h] {response.status_code} response for {customer_id=}: {response.json()}"
    )


def shutdown(signum, frame):
    logger.info("Shutting down")
    stop_event.set()


if __name__ == "__main__":
    KAFKA_BROKER = os.environ["KAFKA_BROKER"]
    SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
    FEATURE_STORE_URL = os.environ["FEATURE_STORE_URL"]
    main()
