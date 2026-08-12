# Pinot Load

This image is used to run load jobs into Pinot.
It queries Trino for batch table results and then produces them into Kafka where they can then be loaded into Pinot.
Pinot can be configured to upsert records from Kafka meaning only the latest state from the batch load is returned
in Pinot queries.
