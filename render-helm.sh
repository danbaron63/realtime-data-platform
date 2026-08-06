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

# Pinot
PINOT_VERSION=${3:-1.0.0}
echo "Rendering Pinot version ${PINOT_VERSION}"

helm repo add pinot https://raw.githubusercontent.com/apache/pinot/master/helm
helm repo update

helm template \
  --version ${PINOT_VERSION} \
  -f pinot-values.yaml \
  pinot pinot/pinot \
  > kube/pinot/helm.yaml

# Prometheus
PROMETHEUS_VERSION=88.1.5

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts

helm template \
  --version ${PROMETHEUS_VERSION} \
  -f kube-prometheus-stack.yaml \
  prometheus prometheus-community/kube-prometheus-stack \
  > kube/metrics/helm.yaml

helm show \
  crds \
  --version ${PROMETHEUS_VERSION} \
  prometheus-community/kube-prometheus-stack \
  > kube/metrics/crds.yaml

echo "Done!"