import argparse
import os
import time

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr
from pyspark.sql.streaming import StreamingQuery


def get_latest_schema_from_registry(registry_url: str, topic_name: str) -> str:
    """
    Fetches the latest Avro schema string from Redpanda Schema Registry.
    Confluent defaults to the 'TopicNameStrategy', meaning the subject
    is always named `<topic>-value` for the message payload.
    """
    subject = f"{topic_name}-value"
    url = f"{registry_url}/subjects/{subject}/versions/latest"

    logger.warn(f"fetching schema {subject} from schema registry")

    try:
        response = requests.get(url)
        response.raise_for_status()

        schema_data = response.json()
        return schema_data["schema"]

    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Failed to fetch schema for subject {subject}: {e}")


def process_stream(topic: str) -> StreamingQuery:
    logger.warn(f"Creating stream for {topic}")
    avro_schema_str = get_latest_schema_from_registry(SCHEMA_REGISTRY_URL, topic)

    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", REDPANDA_BROKERS)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    df_structured = (
        df_raw.withColumn("pure_avro_bytes", expr("substring(value, 6)"))
        .withColumn("data", from_avro(col("pure_avro_bytes"), avro_schema_str))
        .select("data.*")
    )

    iceberg_target = topic.split(".")[-1]

    table_name = f"polaris.raw.{iceberg_target}"

    def merge_dataframe(df: DataFrame, batch_id: int):
        session = df.sparkSession

        df.createOrReplaceTempView("batch")

        session.sql(f"""
            MERGE INTO {table_name} t
            USING batch s
            ON t.id = s.id
            WHEN NOT MATCHED THEN
              INSERT *
        """)

    if not spark.catalog.tableExists(table_name):
        empty_df = spark.createDataFrame(
            data=[],
            schema=df_structured.schema
        )
        empty_df.writeTo(table_name).using("iceberg").create()

    logger.warn(f"starting query: {table_name}")
    query = (
        df_structured.writeStream.queryName(topic)
        .option("checkpointLocation", f"s3a://lakehouse/checkpoints/{topic}_stream/")
        .trigger(processingTime="1 minute")
        .foreachBatch(merge_dataframe)
        .start()
    )

    return query


def wait_for_dependencies(urls: list[str]):
    backoff = 1.0
    timeout = 600
    timeout_time = time.time() + timeout

    while True:
        if time.time() > timeout_time:
            raise RuntimeError(f"Timed out waiting for dependencies: {urls}")

        successful = 0
        for url in urls:
            try:
                response = requests.get(url)
                response.raise_for_status()
                successful += 1
                logger.warn(f"Successful response from {url}")
            except requests.HTTPError as err:
                logger.error(f"Error response from {url}: {err}")

        if len(urls) == successful:
            return

        logger.warn(f"Backing off for {backoff} seconds, waiting for 200 responses from {urls}")
        time.sleep(backoff)
        backoff *= 1.2

def main(topics: list[str]):
    logger.warn("Creating namespace raw")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.raw")

    logger.warn(f"Ingesting from topics: {topics}")
    for topic in topics:
        process_stream(topic)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    REDPANDA_BROKERS = os.environ["REDPANDA_BROKERS"]
    SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
    POLARIS_ADMIN_URL = os.environ["POLARIS_ADMIN_URL"]
    POLARIS_CREDENTIAL = os.environ["POLARIS_CREDENTIAL"]
    parser = argparse.ArgumentParser()
    parser.add_argument("topics", nargs="+", help="List of topics to consume")
    args = parser.parse_args()
    spark = (
        SparkSession.builder.appName("ingest")
        .config("spark.sql.catalog.polaris.credential", POLARIS_CREDENTIAL)
        .getOrCreate()
    )
    logger = spark._jvm.org.apache.logging.log4j.LogManager.getLogger("python")
    wait_for_dependencies([
        f"{SCHEMA_REGISTRY_URL}/subjects",
        f"{POLARIS_ADMIN_URL}/q/health",
    ])
    main(args.topics)
