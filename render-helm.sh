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

# Spark operator
SPARK_OPERATOR_VERSION=${1:-1.7.0}
echo "Rendering Spark Operator version ${SPARK_OPERATOR_VERSION}"

helm template \
  --version ${SPARK_OPERATOR_VERSION} \
  -f spark-operator-values.yaml \
  spark spark/spark-kubernetes-operator \
  > kube/spark-operator/helm.yaml

helm show \
  crds \
  --version ${SPARK_OPERATOR_VERSION} \
  spark/spark-kubernetes-operator \
  > kube/spark-operator/crds.yaml

echo "Done!"