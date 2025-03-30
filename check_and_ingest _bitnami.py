import os
import requests
import json
import pendulum
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import Variable

# -----------------------------------------------------------------------------
# DAG Configuration
# -----------------------------------------------------------------------------
local_tz = pendulum.timezone("America/New_York")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, tzinfo=local_tz),
    'catchup': False
}

dag = DAG(
    dag_id='ingest_bitnami_vulndb_artifactory',
    default_args=default_args,
    schedule_interval='0 8 * * *',  # Runs daily at 8 AM ET
    description='Recursively ingest all JSON files from Bitnami vulndb Artifactory into Azure Blob Storage (vulbitnami).'
)

# -----------------------------------------------------------------------------
# Helper Function: Recursively list all JSON files from a GitHub API URL
# -----------------------------------------------------------------------------
def list_json_files_recursive(api_url):
    """
    Recursively traverse a GitHub API directory URL to list all JSON files.
    Returns a list of file metadata dictionaries with keys such as 'name' and 'download_url'.
    """
    files = []
    resp = requests.get(api_url)
    if resp.status_code != 200:
        raise ValueError(
            f"Failed to fetch content from GitHub API. Status code: {resp.status_code}, URL: {api_url}"
        )
    items = resp.json()
    for item in items:
        if item.get("type") == "file" and item["name"].endswith(".json"):
            files.append(item)
        elif item.get("type") == "dir":
            files.extend(list_json_files_recursive(item["url"]))
    return files

# -----------------------------------------------------------------------------
# Python Callable:
# - Recursively list all JSON files in the Bitnami vulndb repo's 'data/artifactory' folder.
# - Download each file and upload to Azure Blob Storage (vulbitnami).
# - Update the Airflow Variable with the list of ingested files.
# -----------------------------------------------------------------------------
def ingest_bitnami_vulndb(**kwargs):
    # Use the GitHub API to list contents of the 'data/artifactory' directory.
    github_api_url = "https://api.github.com/repos/bitnami/vulndb/contents/data/artifactory?ref=main"
    json_files = list_json_files_recursive(github_api_url)
    
    if not json_files:
        print("No JSON files found in the Artifactory directory.")
        return

    print(f"Found {len(json_files)} JSON file(s) to ingest:", [f["name"] for f in json_files])

    wasb_hook = WasbHook(wasb_conn_id='azure_blob_conn')
    ingested_files = []

    for file_info in json_files:
        file_name = file_info["name"]
        download_url = file_info["download_url"]

        print(f"Downloading {file_name} from {download_url} ...")
        file_resp = requests.get(download_url)
        if file_resp.status_code != 200:
            print(f"WARNING: Could not download {file_name}. Skipping.")
            continue

        local_file_path = os.path.join("/tmp", file_name)
        with open(local_file_path, "wb") as f:
            f.write(file_resp.content)

        print(f"Uploading {file_name} to Azure Blob Storage (container: vulbitnami)...")
        wasb_hook.load_file(
            file_path=local_file_path,
            container_name="vulbitnami",
            blob_name=file_name,
            overwrite=True
        )
        print(f"Uploaded {file_name} successfully.")
        ingested_files.append(file_name)

        try:
            os.remove(local_file_path)
        except OSError:
            pass

    Variable.set("ingested_bitnami_files", json.dumps(ingested_files))
    print("Airflow Variable updated with the list of ingested Bitnami files.")

# -----------------------------------------------------------------------------
# PythonOperator Task
# -----------------------------------------------------------------------------
ingest_bitnami_task = PythonOperator(
    task_id='ingest_bitnami_vulndb_jsons',
    python_callable=ingest_bitnami_vulndb,
    dag=dag
)
