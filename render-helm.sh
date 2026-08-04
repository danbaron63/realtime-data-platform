#!/bin/bash


helm repo add spark https://apache.github.io/spark-kubernetes-operator
helm repo add redpanda https://charts.redpanda.com
helm repo update

# RedPanda
REDPANDA_VERSION="5.8.0"
CONSOLE_VERSION="1.2.0"

echo "Rendering Redpanda v${REDPANDA_VERSION}..."

helm template \
  redpanda redpanda/redpanda \
  --version ${REDPANDA_VERSION} \
  --namespace default \
  -f redpanda-values.yaml \
  > kube/redpanda/redpanda.yaml

echo "Done!"