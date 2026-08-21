# spark-jobs

Python build of spark jobs with dependencies.

## Raw Ingestion Job
This job is a streaming job that ingests records from Kafka (`raw`) and writes them to Iceberg tables for batch processing.
It makes use of upserts to ensure there are no duplicates in Iceberg.
This is particularly useful if the Spark checkpoints get corrupted, we can restart the stream with a new checkpoint location
and only new records will be written.