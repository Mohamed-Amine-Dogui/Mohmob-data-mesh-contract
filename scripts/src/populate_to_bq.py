from google.cloud import bigquery
import yaml
import json
import os
import logging
from datetime import datetime, timezone, date
import time
from pathlib import Path
from google.cloud.exceptions import NotFound

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def read_yaml(file_path):
    try:
        with open(file_path, "r") as file:
            return yaml.safe_load(file)
    except Exception as e:
        logging.error(f"Error reading YAML file {file_path}: {e}")
        raise


def serialize_to_json(data):
    try:
        return json.dumps(data).replace('\\"', '"')
    except TypeError as e:
        logging.error(f"Error serializing to JSON: {e}")
        raise


def extract_data_contract(data):
    try:
        input_ports = data.get("inputs", [])
        if not isinstance(input_ports, list):
            input_ports = [input_ports]
        dpc_input_port = [serialize_to_json(port) for port in input_ports]

        output_port = data.get("output", {}).get("storage", {})
        dpc_output_port = serialize_to_json(output_port)

        quality_checks = data.get("quality", [])
        if not isinstance(quality_checks, list):
            quality_checks = [quality_checks]
        dpc_quality = [serialize_to_json(check) for check in quality_checks]

        return {
            "dpc_id": data["metadata"]["contractId"],
            "dpc_name": data["metadata"]["contractName"],
            "dpc_version": data["metadata"]["version"],
            "dpc_input_port": dpc_input_port,
            "dpc_output_port": dpc_output_port,
            "dpc_quality": dpc_quality,
        }
    except KeyError as e:
        logging.error(f"Missing key in data contract: {e}")
        raise
    except (TypeError, json.JSONDecodeError) as e:
        logging.error(f"Error serializing or cleaning JSON data: {e}")
        raise


def extract_data_product(data):
    try:
        return {
            "dp_id": data["id"],
            "dp_name": data["name"],
            "dp_owner_dataDomain": data["dataProductOwner"]["dataDomain"],
            "dp_owner_contact": data["dataProductOwner"]["contact"],
            "da_owner_dataDomain": data.get("dataAssetOwner", {}).get("dataDomain", ""),
            "da_owner_contact": data.get("dataAssetOwner", {}).get("contact", ""),
            "dp_maturity": data["maturity"],
        }
    except KeyError as e:
        logging.error(f"Missing key in data product: {e}")
        raise


def delete_table(table_id):
    try:
        client = bigquery.Client()
        client.delete_table(table_id)
        logging.info(f"Table {table_id} has been deleted.")
    except Exception as e:
        logging.error(f"Error deleting BigQuery table {table_id}: {e}")
        raise e

def table_exist(client, table_id: str) -> bool:
    try:
        client.get_table(table_id)  # Make an API request.
        return True
    except NotFound:
        return False

def create_metadata_snapshot_table(project_id, dataset_id, table_id, schema_file_path):
    """
    Create the table metadata_snapshot in a BigQuery dataset using a schema from a JSON file.
    """
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    try:
        with open(schema_file_path, "r") as file:
            schema_definition = json.load(file)
    except Exception as e:
        logging.error(f"Failed to read schema from {schema_file_path}: {e}")
        raise

    schema = [
        bigquery.SchemaField(
            name=field["name"],
            field_type=field["type"],
            mode=field.get("mode", "NULLABLE"),
            description=field.get("descriptions", ""),
        )
        for field in schema_definition
    ]
    table = bigquery.Table(table_ref, schema=schema)

    try:
        table = client.create_table(table)
        logging.info(f"Created table {table.project}.{table.dataset_id}.{table.table_id}")
    except Exception as e:
        logging.error(f"Failed to create table {table_ref}: {e}")

    while True:
        if table_exist(client,table_ref ):
            break
        else:
            time.sleep(10)


def ingest_in_bigquery(data, table_id):
    try:
        client = bigquery.Client()
        table = client.get_table(table_id)  # Ensure the table exists
        errors = client.insert_rows_json(table, data)
        if not errors:
            logging.info("New rows have been added.")
        else:
            logging.error(f"Encountered errors while inserting rows: {errors}")
            raise Exception(f"Insertion errors: {errors}")
    except Exception as e:
        logging.error(f"Error appending data to BigQuery table {table_id}: {e}")
        raise


def construct_dataset_name(base_name, suffix):
    return f"{base_name}_{suffix}" if suffix in ["dev", "pre"] else base_name


def construct_table_ids(gcp_project, dataset):
    return {
        "metadata_snapshot": f"{gcp_project}.{dataset}.metadata_snapshot",
        "metadata_history": f"{gcp_project}.{dataset}.metadata_history",
    }


def populate_data(
    all_metadata_snapshots,
    all_metadata_histories,
    table_ids,
    project_id,
    dataset_id,
    schema_file_path,
):
    try:
        delete_table(table_ids["metadata_snapshot"])
        create_metadata_snapshot_table(
            project_id, dataset_id, "metadata_snapshot", schema_file_path
        )
        ingest_in_bigquery(all_metadata_snapshots, table_ids["metadata_snapshot"])
        ingest_in_bigquery(all_metadata_histories, table_ids["metadata_history"])
    except Exception as e:
        logging.error(f"Error populating data to BigQuery: {e}")
        raise


def process_files(data_product_file, data_contract_files):
    try:
        extracted_data_product = extract_data_product(read_yaml(data_product_file))
        all_metadata_snapshots = []
        all_metadata_histories = []

        for data_contract_file in data_contract_files:
            extracted_data_contract = extract_data_contract(
                read_yaml(data_contract_file)
            )
            metadata = {**extracted_data_product, **extracted_data_contract}
            current_date = date.today().isoformat()
            metadata_snapshot = {
                **metadata,
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "date": current_date,
            }
            metadata_history = {
                **metadata,
                "ingested_timestamp": datetime.now(timezone.utc).isoformat(),
                "date": current_date,
            }

            all_metadata_snapshots.append(metadata_snapshot)
            all_metadata_histories.append(metadata_history)

        return all_metadata_snapshots, all_metadata_histories
    except Exception as e:
        logging.error(f"Error processing files: {e}")
        raise


def get_env_or_exception(var_name):
    env_value = os.getenv(var_name)
    if not env_value:
        raise Exception(f"Environment value is not set for {var_name}")
    return env_value


def main():
    """
    Main function to process data product and data contract files, and populate data into BigQuery.
    The function performs the following steps:
    1. Sets the base path to the directory containing data products.
    2. Constructs the dataset name using the provided environment suffix.
    3. Iterates through each product directory within the base path.
    4. For each product directory under 'dataproducts':
       a. Constructs the path to the data product YAML file.
       b. Constructs the path to the 'contracts' subdirectory.
       c. Gathers all YAML files within the 'contracts' subdirectory.
       d. Checks if the data product file exists and if there are any contract files.
       e. Calls the process_files function to process the data and collect metadata.
    5. Delete the snapshot table to avoid the buffer error and recreating it
    6. ingest data in both table snapshot and history
    """
    try:
        base_path = Path("../../dataproducts")
        snapshot_schema_path = Path(
            "../../iac/bq_schemas_static/data_mesh_contracts/metadata_snapshot.tpl"
        )
        env_suffix = get_env_or_exception("DATASET_SUFFIX")
        gcp_project = get_env_or_exception("PROJECT_NAME")

        dataset_name = construct_dataset_name("data_mesh_contracts", env_suffix)

        all_metadata_snapshots = []
        all_metadata_histories = []

        for product_dir in base_path.iterdir():
            if product_dir.is_dir():
                data_product_file = product_dir / f"{product_dir.name}.yaml"
                data_contracts_dir = product_dir / "contracts"
                data_contract_files = list(data_contracts_dir.glob("*.yaml"))
                if data_product_file.exists() and data_contract_files:
                    metadata_snapshots, metadata_histories = process_files(
                        data_product_file,
                        data_contract_files,
                    )
                    all_metadata_snapshots.extend(metadata_snapshots)
                    all_metadata_histories.extend(metadata_histories)

        table_ids = construct_table_ids(gcp_project, dataset_name)

        populate_data(
            all_metadata_snapshots,
            all_metadata_histories,
            table_ids,
            gcp_project,
            dataset_name,
            snapshot_schema_path,
        )

    except Exception as e:
        logging.error(f"Error in main: {e}")
        raise


if __name__ == "__main__":
    main()
