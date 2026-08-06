from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
import requests
import boto3
import argparse


BOOTSTRAP_SERVERS = "redpanda-0.redpanda.default.svc.cluster.local:9093"
SCHEMA_REGISTRY_URL = "http://redpanda-0.redpanda.default.svc.cluster.local:8081"
MINIO_PASSWORD = "minioadmin"
BUCKET = "lakehouse"
POLARIS_BASE_URL = "http://localhost:8181/api"
POLARIS_CLIENT_ID = "root"
POLARIS_CLIENT_SECRET = "s3cr3td"
POLARIS_CATALOG_NAME = "default_catalog"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topics", action="store_true", default=False)
    parser.add_argument("--schemas", action="store_true", default=False)
    parser.add_argument("--namespace", "-n", default="", help="kafka topic namespace")
    parser.add_argument("--checkpoints", action="store_true", default=False)
    parser.add_argument("--data", action="store_true", default=False)
    parser.add_argument("--polaris-namespace", default="")
    args = parser.parse_args()
    namespace = args.namespace
    if args.topics:
        delete_topics(namespace)
    if args.schemas:
        delete_schemas(namespace)
    if args.checkpoints:
        delete_checkpoints()
    if args.data:
        delete_data(args.polaris_namespace)


def delete_topics(namespace: str) -> None:
    admin_client = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    topics_metadata = admin_client.list_topics()
    topics = topics_metadata.topics.keys()
    topics_to_delete = [
        t for t in topics if not t.startswith("_") and t.startswith(namespace)
    ]

    if len(topics_to_delete) == 0:
        print("No topics to delete")
        return

    print(f"Deleting topics: {topics_to_delete}")
    result = admin_client.delete_topics(topics_to_delete)
    for topic, future in result.items():
        future.result()
        print(f"{topic} deleted")


def delete_schemas(namespace: str) -> None:
    registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    subjects = registry_client.get_subjects()
    if len(subjects) == 0:
        print("No subjects to delete")
        return

    for subject in subjects:
        if subject.startswith(namespace):
            response = registry_client.delete_subject(subject)
            print(f"Deleted {subject} - versions deleted: {response}")


def delete_data(polaris_namespace: str) -> None:
    if polaris_namespace == "":
        raise Exception("must provide polaris namespace when deleting tables")
    token = get_token()
    tables = get_tables(token, POLARIS_CATALOG_NAME, polaris_namespace)
    for table in tables:
        response = get_table(token, POLARIS_CATALOG_NAME, polaris_namespace, table)
        location = response["metadata"]["location"].strip("s3a://")
        delete_table(token, POLARIS_CATALOG_NAME, polaris_namespace, table)
        delete_objects(location)
        print(f"data at {location} deleted")


def delete_checkpoints() -> None:
    delete_objects("checkpoints/")
    print("checkpoints/ deleted")


def delete_objects(prefix: str, continuation_token: str | None = None):
    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=MINIO_PASSWORD,
        aws_secret_access_key=MINIO_PASSWORD,
        endpoint_url="http://localhost:9000",
    )

    args = dict(
        Bucket=BUCKET,
        Prefix=prefix,
    )
    if continuation_token:
        args["ContinuationToken"] = continuation_token

    response = s3.list_objects_v2(
        **args,
    )
    if "Contents" not in response:
        return

    objects = [dict(Key=o["Key"]) for o in response["Contents"]]
    s3.delete_objects(
        Bucket=BUCKET,
        Delete=dict(
            Objects=objects,
            Quiet=False,
        ),
    )
    if "NextContinuationToken" in response:
        delete_objects(prefix, response["NextContinuationToken"])


def get_token() -> str:
    print("Getting polaris token")
    response = requests.post(
        f"{POLARIS_BASE_URL}/catalog/v1/oauth/tokens",
        data=dict(
            grant_type="client_credentials",
            client_id=POLARIS_CLIENT_ID,
            client_secret=POLARIS_CLIENT_SECRET,
            scope="PRINCIPAL_ROLE:ALL",
        ),
    )
    response.raise_for_status()
    polaris_token = response.json()["access_token"]
    return polaris_token


def get_tables(token: str, catalog: str, namespace: str) -> list[str]:
    response = requests.get(
        url=f"{POLARIS_BASE_URL}/catalog/v1/{catalog}/namespaces/{namespace}/tables",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    response.raise_for_status()
    return [t["name"] for t in response.json()["identifiers"]]


def get_table(token: str, catalog: str, namespace: str, table: str) -> dict:
    response = requests.get(
        url=f"{POLARIS_BASE_URL}/catalog/v1/{catalog}/namespaces/{namespace}/tables/{table}",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    response.raise_for_status()
    return response.json()


def delete_table(token: str, catalog: str, namespace: str, name: str):
    response = requests.delete(
        url=f"{POLARIS_BASE_URL}/catalog/v1/{catalog}/namespaces/{namespace}/tables/{name}",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    if response.status_code == 404:
        print(f"Table '{catalog}.{namespace}.{name}' not found")
        return
    response.raise_for_status()
    print(f"Table '{catalog}.{namespace}.{name}' deleted")


if __name__ == "__main__":
    main()
