import argparse
import json
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from datetime import datetime, timezone
import logging
import os
from trino.dbapi import connect
from typing import Generator, Any


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract(query: str) -> Generator[dict[str, Any], None, None]:
    logger.info(f"extract: {query=}")
    conn = connect(
        host="trino",
        port="8080",
        user="spark",
        catalog="polaris",
        schema="features",
    )
    cur = conn.cursor()
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    return (dict(zip(columns, r)) for r in cur)


def create_topic(namespace: str, topic: str):
    namespaced_topic = f"{namespace}.{topic}"
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})

    metadata = admin_client.list_topics(timeout=5)
    if namespaced_topic in metadata.topics:
        logger.info(f"Topic {namespaced_topic} already exists")
        return

    logger.info(f"Provisioning topic {namespaced_topic}")
    new_topic = NewTopic(
        topic=namespaced_topic,
        num_partitions=3,
        replication_factor=1,
    )

    futures = admin_client.create_topics([new_topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info(f"Successfully created topic: {topic_name}")
        except Exception as e:
            raise Exception(f"Failed to create topic {namespaced_topic}") from e


def process_events(events: Generator[dict[str, Any], None, None], namespace: str, topic_name: str):
    create_topic(namespace, topic_name)
    full_topic_name = f"{namespace}.{topic_name}"

    with open(f"{SCHEMAS_DIR}/{topic_name}.avsc", "r") as f:
        schema = f.read()

    schema_json = json.loads(schema)
    primary_key_columns = schema_json["meta-primary-key"]

    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    avro_serialiser = AvroSerializer(schema_registry_client, schema)

    producer = Producer({
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",
        "enable.idempotence": True,
        "retries": 5,
        "delivery.timeout.ms": 30_000,
    })

    def delivery_callback(err, _):
        if err is not None:
            logger.error(f"message delivery failed: {err}")

    for event in events:
        producer.poll(0.0)

        try:
            serialised_value = avro_serialiser(
                sanitize_record(event),
                SerializationContext(full_topic_name, MessageField.VALUE)
            )
            key = "|".join([event[c] for c in primary_key_columns])

            producer.produce(
                topic=full_topic_name,
                key=key,
                value=serialised_value,
                on_delivery=delivery_callback,
            )
        except Exception as e:
            raise Exception("Error during serialisation/production") from e

    producer.flush()
    logger.info("finished writing records to kafka")


def sanitize_record(record: dict[str, Any]) -> dict[str, Any]:
    for key, value in record.items():
        if isinstance(value, datetime):
            record[key] = int(value.replace(tzinfo=timezone.utc).timestamp() * 1000)
    return record


def snake_to_camel_case(string: str) -> str:
    first, *rest = string.split("_")
    return first + "".join([word.lower().capitalize() for word in rest])


if __name__ == "__main__":
    KAFKA_BROKER = os.environ["KAFKA_BROKER"]
    SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
    SCHEMAS_DIR = os.environ["SCHEMAS_DIR"]

    namespace = "featureStore.staging"

    parser = argparse.ArgumentParser()
    parser.add_argument("tables", nargs="+", help="List of tables to extract")
    args = parser.parse_args()

    for table in args.tables:
        topic_name = snake_to_camel_case(table)
        logger.info(f"Reading from table {table} and pushing to topic {topic_name}")

        records = extract(f"SELECT * FROM {table}")
        process_events(records, namespace, topic_name)
