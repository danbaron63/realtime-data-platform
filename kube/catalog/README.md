
# Commands

## Get auth token
```
curl -X POST http://localhost:8181/api/catalog/v1/oauth/tokens \
  -H "X-Iceberg-Access-Realm: POLARIS" \
  -d "grant_type=client_credentials" \
  -d "client_id=root" \
  -d "client_secret=s3cr3td" \
  -d "scope=PRINCIPAL_ROLE:ALL"
```

Then `export POLARIS_TOKEN=...`.

## Create catalog
```
curl -i -X POST http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "X-Iceberg-Access-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{
    "catalog": {
      "name": "default_catalog",
      "type": "INTERNAL",
      "properties": {
        "default-base-location": "s3://lakehouse/warehouse",
        "s3.access-key-id": "minioadmin",
        "s3.secret-access-key": "minioadmin"
      },
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": [
          "s3://lakehouse/warehouse"
        ],
        "endpoint": "http://minio.default.svc.cluster.local:9000",
        "pathStyleAccess": true,
        "stsUnavailable": true
      }
    }
  }'
```

Check it exists:
```
curl -i -X GET http://localhost:8181/api/management/v1/catalogs \
  -H "Authorization: Bearer ${POLARIS_TOKEN}" \
  -H "X-Iceberg-Access-Realm: POLARIS" \
  -H "Content-Type: application/json"
```

## Create namespace

```
curl -i -X POST http://localhost:8181/api/catalog/v1/default_catalog/namespaces \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "X-Iceberg-Access-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{
    "namespace": ["dev_db"],
    "properties": {
      "description": "Development database space"
    }
  }'
```

Check it exists:

```
curl -i -X GET http://localhost:8181/api/catalog/v1/default_catalog/namespaces \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "X-Iceberg-Access-Realm: POLARIS" \
  -H "Content-Type: application/json"  
```

## Create table

```
curl -i -X POST http://localhost:8181/api/catalog/v1/default_catalog/namespaces/dev_db/tables \
  -H "Authorization: Bearer $POLARIS_TOKEN" \
  -H "X-Iceberg-Access-Realm: POLARIS" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test_table",
    "location": "s3://lakehouse/warehouse/dev_db/test_table",
    "schema": {
      "type": "struct",
      "fields": [
        { "id": 1, "name": "id", "required": true, "type": "int" },
        { "id": 2, "name": "data", "required": false, "type": "string" }
      ]
    }
  }'
```
