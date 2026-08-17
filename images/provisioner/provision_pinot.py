import argparse
import json
import logging
import os
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_json(path: str) -> dict:
    with open(path, "r") as f:
        return json.load(f)


def upsert_schema(schema: dict, controller_url: str):
    name = schema["schemaName"]
    url = f"{controller_url}/schemas"
    logger.info(f"upsert_schema: using url {url}")

    logger.info(f"Checking if schema '{name}' already exists")
    check = requests.get(f"{controller_url}/schemas/{name}")

    if check.status_code == 200:
        logger.info(f"Schema '{name}' exists, updating")
        response = requests.put(f"{url}/{name}", json=schema)
    else:
        logger.info(f"Schema '{name}' not found, creating")
        response = requests.post(url, json=schema)

    if not response.ok:
        logger.error(f"Failed to provision schema '{name}': {response.status_code} {response.text}")
    response.raise_for_status()
    logger.info(f"Schema '{name}' provisioned successfully")


def upsert_table(table: dict, controller_url: str):
    name = table["tableName"]
    table_type = table["tableType"]
    full_name = f"{name}_{table_type}".lower()
    url = f"{controller_url}/tables"

    logger.info(f"Checking if table '{full_name}' already exists")
    check = requests.get(f"{url}/{name}")

    if check.status_code == 200:
        logger.info(f"Table '{full_name}' exists, updating")
        response = requests.put(f"{url}/{name}", json=table)
    else:
        logger.info(f"Table '{full_name}' not found ({check.status_code}), creating")
        response = requests.post(url, json=table)

    response.raise_for_status()
    logger.info(f"Table '{full_name}' provisioned successfully")


def get_tables(controller_url: str) -> list[str]:
    url = f"{controller_url}/tables"

    response = requests.get(url)

    try:
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch tables: {response.text}")
        raise e

    tables = response.json()["tables"]
    return tables


def delete_table(controller_url: str, table_name: str):
    logger.warning(f"deleting {table_name=}")
    url = f"{controller_url}/tables/{table_name}"

    response = requests.delete(url)

    try:
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to delete table {table_name}: {response.text}")
        raise e


def delete_schema(controller_url: str, schema: str):
    logger.warning(f"deleting {schema=}")
    url = f"{controller_url}/schemas/{schema}"

    response = requests.delete(url)

    try:
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to delete schema {schema}: {response.text}")
        raise e


def get_schemas(controller_url: str) -> list[str]:
    url = f"{controller_url}/schemas"

    response = requests.get(url)

    try:
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch schemas: {response.text}")
        raise e

    tables = response.json()
    return tables


def main(config_dir: str, controller_url: str):
    logger.info(f"Provisioning Pinot resources from {config_dir} with controller_url {controller_url}")

    configs = [load_json(os.path.join(config_dir, f)) for f in os.listdir(config_dir) if f.endswith(".json")]
    schemas = [c for c in configs if "schemaName" in c]
    tables = [c for c in configs if "tableName" in c]

    existing_tables = set(get_tables(controller_url))
    new_table_names = set([t["tableName"] for t in tables])
    delete_tables = existing_tables - new_table_names
    for table in delete_tables:
        delete_table(controller_url, table)

    existing_schemas = set(get_schemas(controller_url))
    new_schemas_names = set([s["schemaName"] for s in schemas])
    delete_schemas = existing_schemas - new_schemas_names

    exceptions = []

    for schema in delete_schemas:
        try:
            delete_schema(controller_url, schema)
        except Exception as e:
            exceptions.append(e)

    # Ensure schemas are upserted before tables
    for schema in schemas:
        try:
            upsert_schema(schema, controller_url)
        except Exception as e:
            exceptions.append(e)

    for table in tables:
        try:
            upsert_table(table, controller_url)
        except Exception as e:
            exceptions.append(e)

    if exceptions:
        for e in exceptions:
            logger.error(e)
        raise ExceptionGroup("Some pinot operations failed", tuple(exceptions))
    logger.info("Pinot provisioning complete")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--controller", required=True, help="The URL to the pinot controller")
    parser.add_argument("--config", required=True, help="The path to the pinot config file")
    args = parser.parse_args()
    main(args.config, args.controller)
