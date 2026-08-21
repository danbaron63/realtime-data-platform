# Apache Pinot

Apache Pinot is the real time OLAP database and query engine we use for generating online features for the feature store.

## Components
* Pinot is deployed via the [`helm.yaml` file](helm.yaml)  which is templated from Helm - this should not be updated manually.
* Pinot load cron job regularly loads batch data in Pinot for online serving.
* Provision pinot job provisions tables and schemas in Pinot - this runs the [provisioner image](../../images/provisioner).
