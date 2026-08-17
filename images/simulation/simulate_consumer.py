import logging
import os

import httpx
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from prometheus_client import Histogram, start_http_server

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    start_http_server(8000)
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "simulator",
            "auto.offset.reset": "latest",
        }
    )

    consumer.subscribe(["raw.payment_authorised"])
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserialiser = AvroDeserializer(schema_registry_client)
    http_client = httpx.Client(timeout=15)

    while True:
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
        get_payment_features(account_id, http_client)
        get_customer_payment_features(customer_id, http_client)


def get_payment_features(account_id: str, http_client: httpx.Client):
        response = http_client.post(
            f"{FEATURE_STORE_URL}/features/payment_6h",
            headers={
                "Content-Type": "application/json",
            },
            json={"entity_keys": {"account_id": account_id}},
        )
        logger.info(
            f"[payment_6h] {response.status_code} response for {account_id=}: {response.json()}"
        )


def get_customer_payment_features(customer_id: str, http_client: httpx.Client):
        response = http_client.post(
            f"{FEATURE_STORE_URL}/features/customer_payment_6h",
            headers={
                "Content-Type": "application/json",
            },
            json={"entity_keys": {"customer_id": customer_id}},
        )
        logger.info(
            f"[customer_payment_6h] {response.status_code} response for {customer_id=}: {response.json()}"
        )


if __name__ == "__main__":
    KAFKA_BROKER = os.environ["KAFKA_BROKER"]
    SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
    FEATURE_STORE_URL = os.environ["FEATURE_STORE_URL"]
    main()
