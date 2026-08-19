import datetime
import logging
import os
import signal
import sys
import time
import uuid

import httpx
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from prometheus_client import Histogram, start_http_server

logging.basicConfig(level=logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)
http_client = httpx.Client(timeout=0.5)
AMOUNT = 100.0
CURRENCY = "GBP"
CHANNEL = "TEST"
running = True


def freshness_check(
    avro_serializer: AvroSerializer,
    topic: str,
    producer: Producer,
    freshness_histogram: Histogram,
):
    trace_id = f"TEST_{uuid.uuid4()!s}"
    trace_account_id = f"TEST_{uuid.uuid4()!s}"
    trace_customer_id = f"TEST_{uuid.uuid4()!s}"
    trace_card_id = f"TEST_{uuid.uuid4()!s}"
    trace_merchant_id = f"TEST_{uuid.uuid4()!s}"

    event_creation_ns = time.perf_counter_ns()

    event = {
        "id": trace_id,
        "timestamp": datetime.datetime.now(datetime.UTC),
        "account_id": trace_account_id,
        "customer_id": trace_customer_id,
        "card_id": trace_card_id,
        "merchant_id": trace_merchant_id,
        "amount": AMOUNT,
        "currency": CURRENCY,
        "channel": CHANNEL,
    }

    serialised_record = avro_serializer(
        event,
        SerializationContext(topic, MessageField.VALUE),
    )

    event_serialisation_ns = time.perf_counter_ns() - event_creation_ns

    producer.produce(
        topic=topic,
        value=serialised_record,
        key=None,
        on_delivery=delivery_callback,
    )
    producer.flush(1.0)

    kafka_acked_time_ns = time.perf_counter_ns()
    kafka_acked_ns = kafka_acked_time_ns - event_creation_ns

    last_request_ns = kafka_acked_time_ns
    requests = 0

    while True:
        requests += 1

        try:
            response = http_client.post(
                f"{FEATURE_STORE_URL}/features/payment_6h",
                headers={
                    "Content-Type": "application/json",
                },
                json={"entity_keys": {"account_id": trace_account_id}},
            )
            response.raise_for_status()
        except httpx.HTTPError as e:
            logger.error(
                f"Error response from feature store ({response.status_code}): {e}"
            )
            continue
        except httpx.TimeoutException as e:
            logger.error(
                f"Timeout waiting for feature store: {e}"
            )
            continue
        finally:
            response_ns = time.perf_counter_ns()
            time_taken_upper_bound_ns = response_ns - event_creation_ns
            time_taken_lower_bound_ns = last_request_ns - event_creation_ns

        features = response.json()
        tx_count = features["tx_count"]
        if tx_count != 0:
            logger.info(
                "request %.0f - event serialised: %.2f ms; kafka acked: %.2f ms; time taken upper bound: %.2f ms; time taken lower bound: %.2f ms. event: %s",
                requests,
                event_serialisation_ns / 1000_000,
                kafka_acked_ns / 1000_000,
                time_taken_upper_bound_ns / 1000_000,
                time_taken_lower_bound_ns / 1000_000,
                features,
            )
            freshness_histogram.labels(
                feature="payment_6h",
            ).observe(time_taken_upper_bound_ns / 1000_000)
            assert tx_count == 1
            break

        last_request_ns = response_ns
        time.sleep(0.005)


def main():
    producer = Producer(
        {
            "bootstrap.servers": KAFKA_BROKER,
        }
    )
    schema_registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    topic = "raw.payment_authorised"
    schema = schema_registry_client.get_latest_version(f"{topic}-value")
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema.schema.schema_str,
    )

    start_http_server(8000)
    freshness_histogram = Histogram(
        "feature_store_freshness_ms",
        "Time from event production to being visible in the feature store",
        labelnames=["feature"],
        buckets=(
            10,
            25,
            50,
            75,
            100,
            125,
            150,
            175,
            200,
            250,
            300,
            350,
            400,
            500,
            600,
            700,
            800,
            1_000,
            1_250,
            1_500,
            2_000,
            5_000,
            10_000,
        ),
    )

    while running:
        time.sleep(10.0)
        freshness_check(avro_serializer, topic, producer, freshness_histogram)


def delivery_callback(err, msg):
    if err:
        logger.error(f"Delivery failed for {msg.topic()}: {err}")


def shutdown(signum, frame):
    logger.info("Shutdown signal received, stopping loop")
    global running
    running = False
    sys.exit(0)


if __name__ == "__main__":
    KAFKA_BROKER = os.environ["KAFKA_BROKER"]
    SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
    FEATURE_STORE_URL = os.environ["FEATURE_STORE_URL"]

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    main()
