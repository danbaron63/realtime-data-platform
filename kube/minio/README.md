# Commands

## Create bucket
```
env AWS_ACCESS_KEY_ID="minioadmin" \
    AWS_SECRET_ACCESS_KEY="minioadmin" \
    AWS_REGION="us-east-1" \
    aws --endpoint-url http://localhost:9000 s3 mb s3://lakehouse
```