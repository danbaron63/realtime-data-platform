# Simulation

Here exists deployments for generating load and probing for data freshness in the feature store.

## Freshness Probe
This deployment generates fake events which are injected into Kafka and then polls the feature store 
with their respective entity keys.
The time between the event being created and being observed in the feature store is measured and 
published as a metric.

## Simulate Consumer
This deployment generates load for the feature store by subscribing to events from Kafka and then
requesting features for those events.
Specifically it subscribes to the `raw.payment_authorised` topic, generating load similar to a fraud
detection service.

### Implementation
In order to prevent high numbers of 404s from the feature store by requesting features that haven't been
ingested yet, this deployment uses a producer/worker model that allows a delay to be introduced whilst maintaining
throughput.

We use 1 producer thread which polls Kafka for new events which are put into a queue; and then 20 worker threads
which consume from the queue, pause for 150ms then request features from the feature store.

We publish metrics about the queue size which ideally should be empty. 
This allows us to know if the producer is producing more records than the workers can collectively consume.