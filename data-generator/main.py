import argparse
import logging
import os
import random
import signal
import struct
import sys
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from dataclasses_avroschema import AvroModel
from generator.customer import CustomerDatabase
from generator.account import AccountDatabase
from generator.card import CardDatabase
from generator.device import DeviceDatabase
from generator.merchant import MerchantDatabase
from generator.payment import PaymentDatabase
from generator.atm import AtmDatabase
from generator.transfer import TransferDatabase
from generator.persistence import Persistence
import requests


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    stream=sys.stdout,
    force=True,
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

event_list = {
    "customer_created": 5,
    "customer_details_updated": 1,
    "account_opened": 5,
    "card_issued": 5,
    "device_registered": 8,
    "login_attempted": 30,
    "merchant_onboarded": 2,
    "payment_authorised": 40,
    "atm_withdrawal": 20,
    "transfer_completed": 25,
}


def main(rate: float):
    wait_for_schema_registry_healthy(300)
    logger.info("Running with rate %s", rate)
    interval_ns = int(1_000_000_000.0 / rate)
    next_event_ns = time.time_ns()

    customer_db = CustomerDatabase()
    account_db = AccountDatabase(customer_db)
    card_db = CardDatabase(customer_db, account_db)
    device_db = DeviceDatabase(customer_db)
    merchant_db = MerchantDatabase()
    payment_db = PaymentDatabase(account_db, card_db, merchant_db)
    atm_db = AtmDatabase(account_db, card_db)
    transfer_db = TransferDatabase(account_db)
    for event_type in event_list:
        provision_topic_if_missing(event_type)

    events = list(event_list.keys())
    weights = list(event_list.values())

    # Seed with some events
    process_event(customer_db.customer_created(), "customer_created")
    process_event(account_db.account_opened(), "account_opened")
    process_event(card_db.card_issued(), "card_issued")
    process_event(merchant_db.merchant_onboarded(), "merchant_onboarded")

    metrics = {e: 0 for e in event_list.keys()}
    count = 0

    while True:
        now_ns = time.time_ns()
        if now_ns < next_event_ns:
            time.sleep((next_event_ns - now_ns) / 1_000_000_000)
            continue
        next_event_ns += interval_ns

        event_type = random.choices(events, weights=weights, k=1)[0]
        metrics[event_type] += 1
        count += 1

        if count % 100 == 0:
            logger.info(f"Events generated: {metrics}")
            metrics = {e: 0 for e in metrics.keys()}

        match event_type:
            case "customer_created":
                event = customer_db.customer_created()
            case "customer_details_updated":
                event = customer_db.customer_details_updated()
            case "account_opened":
                event = account_db.account_opened()
            case "card_issued":
                event = card_db.card_issued()
            case "device_registered":
                event = device_db.device_registered()
            case "login_attempted":
                event = device_db.login_attempted()
            case "merchant_onboarded":
                event = merchant_db.merchant_onboarded()
            case "payment_authorised":
                event = payment_db.payment_authorised()
            case "atm_withdrawal":
                event = atm_db.atm_withdrawal()
            case "transfer_completed":
                event = transfer_db.transfer_completed()
            case _:
                continue

        if event is not None:
            process_event(event, event_type)


def wait_for_schema_registry_healthy(timeout: int, pause_seconds: float = 2, backoff_rate: float = 1.1, max_backoff: float = 15):
    start_time = time.perf_counter()
    while True:
        try:
            response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects", timeout=5)
            response.raise_for_status()
        except requests.exceptions.RequestException as re:
            if time.perf_counter() >= start_time + timeout:
                raise Exception(f"timed out trying to reach schema registry after {timeout} seconds") from re
            logger.warning(f"Schema registry not available, backing off for {pause_seconds:.1f} seconds")
            time.sleep(pause_seconds)
            pause_seconds *= backoff_rate
            pause_seconds = min(pause_seconds, max_backoff)
            continue
        return


def delivery_callback(err, msg):
    if err:
        logger.error(f"Delivery failed for {msg.topic()}: {err}")
    else:
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")


def process_event(event_obj: AvroModel, topic: str):
    # This is the exact topic name you provided
    full_topic = f"raw.{topic}"
    subject = f"{full_topic}-value"

    try:
        # 1. Register the schema and get the ID back
        # This returns the ID directly (as an int) or a Schema object
        result = registry_client.register_schema(
            subject, Schema(event_obj.avro_schema())
        )

        # Normalize the result to get the integer ID
        schema_id = result.schema_id if hasattr(result, "schema_id") else result

        # 2. Get the raw bytes from your dataclass
        raw_avro_bytes = event_obj.serialize()

        # 3. Prepend the Confluent Wire Format (Magic Byte 0 + 4-byte ID)
        # This is required so the consumer knows which schema version was used
        payload = struct.pack(">bI", 0, schema_id) + raw_avro_bytes

        # 4. Produce the correctly formatted payload
        producer.produce(
            topic=full_topic,
            value=payload,
            key=None,
            on_delivery=delivery_callback,
        )
        producer.poll(0)

    except Exception as e:
        logger.info(f"Failed to serialize/produce event for topic {full_topic}: {e}")
        raise e


def provision_topic_if_missing(topic: str):
    """Declares and provisions the Kafka topic inside Redpanda."""
    admin_client = AdminClient({"bootstrap.servers": REDPANDA_BROKERS})

    namespaced_topic = f"raw.{topic}"

    # Check existing topics
    metadata = admin_client.list_topics(timeout=5)
    if namespaced_topic in metadata.topics:
        logger.info(f"Topic '{namespaced_topic}' already exists.")
        return

    logger.info(f"Topic '{namespaced_topic}' not found. Provisioning...")
    new_topic = NewTopic(
        topic=namespaced_topic,
        num_partitions=3,  # High throughput scale testing
        replication_factor=1,  # Single-node broker requirement
    )

    # Execute topic creation
    futures = admin_client.create_topics([new_topic])
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info(f"Successfully created topic: {topic_name}")
        except Exception as e:
            logger.info(f"Failed to create topic {topic_name}: {e}")
            raise e


def handle_shutdown(signum, frame):
    logger.info("Shutdown signal received, flushing producer...")
    producer.flush(timeout=30)
    Persistence.close()
    logger.info("Producer flushed, exiting")
    sys.exit(0)


if __name__ == "__main__":
    REDPANDA_BROKERS = os.environ["REDPANDA_BROKERS"]
    SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
    DATABASE_PATH = os.environ["DATABASE_PATH"]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rate",
        "-r",
        type=float,
        help="The target rate, per second, of messages to generate",
        default=1,
    )
    args = parser.parse_args()

    producer = Producer(
        {
            "bootstrap.servers": REDPANDA_BROKERS,
            "retries": 5,
            "retry.backoff.ms": 500,
            "delivery.timeout.ms": 30_000,
            "enable.idempotence": True,
            "acks": "all",
        }
    )
    registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    Persistence.initialize(DATABASE_PATH)
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    main(args.rate)
