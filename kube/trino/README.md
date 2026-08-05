# Querying with Trino

Run this command to run the Trino CLI:
```
kubectl run -it --rm --image=trinodb/trino:latest trino-cli -- trino --server trino:8080 --catalog polaris
```
