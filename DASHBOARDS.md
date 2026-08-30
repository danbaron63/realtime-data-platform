# Dashboard Screenshots

Below are some screenshots of the Feature Store Grafana dashboard.
These metrics were captured with the `data-generator` producing 4 events/second.

You can view these metrics yourself with minikube running by port-forwarding port `3000` on the `prometheus-grafana-...`
pod in the `metrics` namespace, either in `k9s` or with `kubectl`.

![Feature store metrics](./media/r4/feature-store-dashboard.png)
![Pinot metrics](./media/r4/pinot-dashboard.png)
![Consumer simulator and freshness metrics](./media/r4/consumer-simulator-and-freshness-probe-dashboard.png)
