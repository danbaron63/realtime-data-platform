import boto3
import requests
import argparse
from kubernetes import client, config


minio_host = "minio"
polaris_host = "polaris"
minio_password = "minioadmin"
minio_endpoint = "http://minio.default.svc.cluster.local:9000"
bucket_name = "lakehouse"
base_location = f"s3a://{bucket_name}/warehouse"
polaris_base_url = f"http://{polaris_host}:8181/api"
polaris_client_id = "root"
polaris_client_secret = "s3cr3td"
# polaris_default_header = {"X-Iceberg-Access-Realm": "POLARIS"}
polaris_default_header = {}  # Spark support for realms isn't great, stick with default
polaris_catalog_name = "default_catalog"
polaris_namespace_name = "dev_db"
principal = "spark"
principal_role = "admin_role"
catalog_role = "admin_catalog"


def main(delete_resources: bool, refresh_resources: bool):
    polaris_token = get_token()
    if delete_resources:
        delete(polaris_token)
    elif refresh_resources:
        delete(polaris_token)
        create(polaris_token)
    else:
        create(polaris_token)


def delete(polaris_token: str):
    print("Deleting resources...")
    unassign_catalog_role_to_principal_role(
        polaris_token, polaris_catalog_name, catalog_role, principal_role
    )
    unassign_principal_role(polaris_token, principal, "service_admin")
    delete_principal(polaris_token, principal)

    namespaces = get_namespaces(polaris_token)
    for namespace in namespaces:
        tables = get_tables(polaris_token, polaris_catalog_name, namespace)
        for table in tables:
            delete_table(polaris_token, polaris_catalog_name, namespace, table)
        delete_namespace(polaris_token, polaris_catalog_name, namespace)
    delete_catalog_role(polaris_token, catalog_role, polaris_catalog_name)
    delete_catalog(polaris_token, polaris_catalog_name)
    delete_principal_role(polaris_token, principal_role)
    print("")


def create(polaris_token: str):
    print("Creating resources...")
    configure_minio()
    create_catalog(
        polaris_token,
        polaris_catalog_name,
        base_location,
        minio_password,
        minio_endpoint,
    )
    create_namespace(polaris_token, polaris_namespace_name, polaris_catalog_name)
    create_principal(polaris_token, principal)
    create_principal_role(polaris_token, principal_role)
    create_catalog_role(polaris_token, catalog_role, polaris_catalog_name)
    assign_principal_role(polaris_token, principal, principal_role)
    assign_catalog_role_to_principal_role(
        polaris_token, polaris_catalog_name, catalog_role, principal_role
    )
    assign_catalog_role_grant(
        polaris_token,
        polaris_catalog_name,
        catalog_role,
        "catalog",
        "CATALOG_MANAGE_CONTENT",
    )
    print("")


def request(verb: str, url: str, token: str, body: dict = None):
    headers = {
        "Content-Type": "application/json",
        **polaris_default_header,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    r = dict(method=verb, url=f"{polaris_base_url}/{url}", headers=headers)
    if body:
        r["json"] = body
    response = requests.request(**r)
    return response


def post(url: str, token: str = None, body: dict = None):
    response = request("POST", url, token, body)
    return response


def get(url: str, token: str):
    return request("GET", url, token)


def configure_minio():
    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id=minio_password,
        aws_secret_access_key=minio_password,
        endpoint_url=f"http://{minio_host}:9000",
    )

    bucket_names = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    if bucket_name in bucket_names:
        print("Bucket already exists, skipping creation")
    else:
        s3.create_bucket(Bucket=bucket_name)
        print("Bucket created")


def get_token() -> str:
    print("Getting polaris token")
    response = requests.post(
        f"{polaris_base_url}/catalog/v1/oauth/tokens",
        data=dict(
            grant_type="client_credentials",
            client_id=polaris_client_id,
            client_secret=polaris_client_secret,
            scope="PRINCIPAL_ROLE:ALL",
        ),
        headers=polaris_default_header,
    )
    response.raise_for_status()
    polaris_token = response.json()["access_token"]
    return polaris_token


def get_catalogs(token: str) -> list[str]:
    response = get(
        "management/v1/catalogs",
        token,
    )
    return [c["name"] for c in response.json()["catalogs"]]


def create_catalog(
    token: str, name: str, base_location: str, minio_pw: str, minio_endpoint: str
):
    catalogs = get_catalogs(token)
    if name in catalogs:
        print(f"Catalog '{name}' already exists")
        return
    post(
        url="/management/v1/catalogs",
        token=token,
        body=dict(
            catalog=dict(
                name=name,
                type="INTERNAL",
                properties={
                    "default-base-location": base_location,
                    "s3.access-key-id": minio_pw,
                    "s3.secret-access-key": minio_pw,
                    "polaris.config.drop-with-purge.enabled": "true",
                },
                storageConfigInfo=dict(
                    storageType="S3",
                    allowedLocations=[
                        base_location,
                    ],
                    endpoint=minio_endpoint,
                    pathStyleAccess="true",
                    stsUnavailable="true",
                ),
            )
        ),
    )
    print("Catalog created")


def delete_catalog(token: str, name: str):
    response = request(
        "DELETE",
        url=f"management/v1/catalogs/{name}",
        token=token,
    )
    if response.status_code == 404:
        print(f"Catalog '{name}' does not exist")
        return
    response.raise_for_status()
    print(f"Catalog '{name}' deleted")


def get_namespaces(token: str) -> list[str]:
    response = get(f"catalog/v1/{polaris_catalog_name}/namespaces", token).json()
    if "namespaces" not in response:
        return []
    namespaces = response["namespaces"]
    if len(namespaces) == 0:
        return []
    return namespaces[0]


def create_namespace(token: str, name: str, catalog: str, description: str = ""):
    namespaces = get_namespaces(token)
    if name in namespaces:
        print(f"Namespace '{polaris_namespace_name}' already exists")
        return
    post(
        url=f"catalog/v1/{catalog}/namespaces",
        token=token,
        body=dict(
            namespace=[name],
            properties=dict(
                description=description,
            ),
        ),
    )
    print("Namespace created")


def delete_namespace(token: str, catalog: str, name: str):
    response = request(
        "DELETE",
        url=f"catalog/v1/{catalog}/namespaces/{name}",
        token=token,
    )
    if response.status_code == 404:
        print(f"Namespace '{catalog}.{name}' does not exist")
        return
    response.raise_for_status()
    print(f"Namespace '{catalog}.{name}' created")


def create_table(
    token: str, catalog: str, namespace: str, name: str, location: str, schema: dict
):
    response = post(
        url=f"catalog/v1/{catalog}/namespaces/{namespace}/tables",
        token=token,
        body=dict(
            name=name,
            location=location,
            schema=schema,
        ),
    )
    if response.status_code == 409:
        print(f"Table '{catalog}.{namespace}.{name}' already exists")
        return
    response.raise_for_status()
    print(f"Table '{catalog}.{namespace}.{name}' created")


def get_tables(token: str, catalog: str, namespace: str) -> list[str]:
    response = get(
        url=f"catalog/v1/{catalog}/namespaces/{namespace}/tables",
        token=token,
    )
    response.raise_for_status()
    return [t["name"] for t in response.json()["identifiers"]]


def delete_table(token: str, catalog: str, namespace: str, name: str):
    response = request(
        "DELETE",
        url=f"catalog/v1/{catalog}/namespaces/{namespace}/tables/{name}",
        token=token,
    )
    if response.status_code == 404:
        print(f"Table '{catalog}.{namespace}.{name}' not found")
        return
    response.raise_for_status()
    print(f"Table '{catalog}.{namespace}.{name}' deleted")


def principal_exists(token: str, principal: str) -> bool:
    response = get(
        "/management/v1/principals",
        token,
    )
    existing_principals = [p["name"] for p in response.json()["principals"]]
    return principal in existing_principals


def create_principal(token: str, name: str):
    if principal_exists(token, name):
        print(f"Principal '{name}' already exists")
        return
    response = post(
        url="management/v1/principals",
        token=token,
        body={
            "principal": {
                "name": name,
            },
            "credentialRotationRequired": False,
        },
    )
    print(f"Principal '{name} Created, storing credentials")
    credentials = response.json()["credentials"]
    client_id = credentials["clientId"]
    client_secret = credentials["clientSecret"]
    store_credentials_as_secret(client_id, client_secret)


def delete_principal(token: str, principal: str):
    if not principal_exists(token, principal):
        print(f"Principal '{principal}' does not exist")
        return
    request(
        "DELETE",
        url=f"management/v1/principals/{principal}",
        token=token,
    )
    print(f"Principal '{principal}' deleted")


def create_principal_role(token: str, name: str):
    response = post(
        url="management/v1/principal-roles",
        token=token,
        body=dict(
            principalRole=dict(
                name=name,
                federated=False,
                properties=dict(),
            )
        ),
    )
    if response.status_code == 409:
        print(f"Principal role '{name}' already exists")
        return
    response.raise_for_status()
    print(f"Principal role '{name}' created")


def delete_principal_role(token: str, name: str):
    response = request(
        "DELETE",
        url=f"management/v1/principal-roles/{name}",
        token=token,
    )
    if response.status_code == 404:
        print(f"Principal role '{name}' does not exist")
        return
    response.raise_for_status()
    print(f"Principal role '{name}' delete")


def create_catalog_role(token: str, name: str, catalog: str):
    response = post(
        url=f"management/v1/catalogs/{catalog}/catalog-roles",
        token=token,
        body=dict(
            catalogRole=dict(
                name=name,
                properties=dict(),
            )
        ),
    )
    if response.status_code == 409:
        print(f"Catalog role '{name}' already exists")
        return
    response.raise_for_status()
    print(f"Catalog role '{name}' created")


def delete_catalog_role(token: str, name: str, catalog: str):
    response = request(
        "DELETE",
        url=f"management/v1/catalogs/{catalog}/catalog-roles/{name}",
        token=token,
    )
    if response.status_code == 404:
        print(f"Catalog role '{name}' does not exist")
        return
    response.raise_for_status()
    print(f"Catalog role '{name}' delete")


def get_principal_roles(token: str, principal: str) -> list[str]:
    response = get(
        url=f"management/v1/principals/{principal}/principal-roles",
        token=token,
    )
    if response.status_code == 404:
        return []
    roles = [r["name"] for r in response.json()["roles"]]
    return roles


def assign_principal_role(token: str, principal: str, principal_role_name: str):
    roles = get_principal_roles(token, principal)
    if principal_role_name in roles:
        print(
            f"Role '{principal_role_name}' already assigned to principal '{principal}'"
        )
        return
    response = requests.put(
        f"{polaris_base_url}/management/v1/principals/{principal}/principal-roles",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            **polaris_default_header,
        },
        json=dict(principalRole=dict(name=principal_role_name, federated=False)),
    )
    response.raise_for_status()
    print("Principal role assigned")


def unassign_principal_role(token: str, principal: str, principal_role_name: str):
    roles = get_principal_roles(token, principal)
    if principal_role_name not in roles:
        print(
            f"Role '{principal_role_name}' is not assigned to principal '{principal}'"
        )
        return
    response = request(
        "DELETE",
        url=f"management/v1/principals/{principal}/principal-roles/{principal_role_name}",
        token=token,
    )
    response.raise_for_status()
    print(
        f"deleted principal role '{principal_role_name}' from principal '{principal}'"
    )


def assign_catalog_role_to_principal_role(
    token: str, catalog: str, catalog_role: str, principal_role: str
):
    response = get(
        url=f"management/v1/principal-roles/{principal_role}/catalog-roles/{catalog}",
        token=token,
    )
    response.raise_for_status()
    roles = [r["name"] for r in response.json()["roles"]]
    if catalog_role in roles:
        print(
            f"Catalog role '{catalog_role}' already assigned to principal role '{principal_role}'"
        )
        return
    response = request(
        "PUT",
        url=f"management/v1/principal-roles/{principal_role}/catalog-roles/{catalog}",
        token=token,
        body=dict(
            catalogRole=dict(
                name=catalog_role,
            )
        ),
    )
    response.raise_for_status()
    print(
        f"Catalog role '{catalog_role}' in catalog '{catalog}' assigned to principal role '{principal_role}'"
    )


def unassign_catalog_role_to_principal_role(
    token: str, catalog: str, catalog_role: str, principal_role: str
):
    response = request(
        "DELETE",
        url=f"management/v1/principal-roles/{principal_role}/catalog-roles/{catalog}/{catalog_role}",
        token=token,
    )
    if response.status_code == 404:
        print(
            f"Catalog role '{catalog_role}' assignment to '{principal_role}' not found"
        )
        return
    response.raise_for_status()
    print(
        f"Catalog role '{catalog_role}' in catalog '{catalog}' unassigned from principal role '{principal_role}'"
    )


def assign_catalog_role_grant(
    token: str, catalog: str, catalog_role: str, type: str, privilege: str
):
    url_grants = f"{polaris_base_url}/management/v1/catalogs/{catalog}/catalog-roles/{catalog_role}/grants"
    response = get(
        f"management/v1/catalogs/{catalog}/catalog-roles/{catalog_role}/grants",
        token=token,
    )
    response.raise_for_status()
    privileges = [(p["privilege"], p["type"]) for p in response.json()["grants"]]
    if (privilege, type) in privileges:
        print(
            f"Privilege '{privilege}' on type '{type}' already granted for catalog role '{catalog_role}' in catalog '{catalog}'"
        )
        return
    response = requests.put(
        url_grants,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            **polaris_default_header,
        },
        json={"grant": {"type": type, "privilege": privilege}},
    )
    response.raise_for_status()
    print(
        f"Privilege '{privilege}' on type '{type}' granted for catalog role '{catalog_role}' in catalog '{catalog}'"
    )


def store_credentials_as_secret(client_id: str, client_secret: str):
    config.load_incluster_config()  # works inside a pod
    v1 = client.CoreV1Api()

    secret = client.V1Secret(
        metadata=client.V1ObjectMeta(
            name="polaris-spark-credentials", namespace="default"
        ),
        string_data={
            "client_id": client_id,
            "client_secret": client_secret,
            "credential": f"{client_id}:{client_secret}",  # pre-formatted for spark
        },
    )

    try:
        v1.create_namespaced_secret(namespace="default", body=secret)
        print("Created secret polaris-spark-credentials")
    except client.exceptions.ApiException as e:
        if e.status == 409:
            v1.replace_namespaced_secret(
                namespace="default", name="polaris-spark-credentials", body=secret
            )
            print("Updated secret polaris-spark-credentials")
        else:
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delete", action="store_true", default=False)
    parser.add_argument("--refresh", action="store_true", default=False)
    args = parser.parse_args()
    main(args.delete, args.refresh)
