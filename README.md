# Realtime Data Platform K8s Demo

This project contains a demonstration of how various data platform components can be deployed in a K8s environment
to enable near infinite scale data processing of various data assets for analytics and real time ML.

## Pre-requisites

Please install the following before attempting to run anything in this project:
* MiniKube (or another k8s environment)
* K9s (recommended)
* Kube Tools - `kubectl`
* Helm
* UV
* Docker

### Hardware
I'm personally running a Lenovo ThinkPad with an Intel Ultra 7 14-core CPU & 32GB of memory.
To run _everything_ at once, I would recommend similar a similar number of cores and memory.

## How to run it
1. Start Kubernetes - with MiniKube:
```
minikube start --cpus=10 --memory=18g
```
2. Build images and apply kube config:
```
make apply
```

You can then fire up K9s and watch the various pods come up.

## Develop

### Maintaining the repo

Some of the kube deployments in this repo originate from Helm charts.
To keep the kube deployment consistent we render the Helm charts to YAML before deploying.
To upgrade these charts you can update the various `*-values.yaml` files and run the `render-helm.sh` script
which will update the relevant YAML files in the `kube` directory.
