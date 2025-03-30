import os
import requests
import json
import pendulum
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.models import Variable

# ------------------------------------------------------------------------
# 1. DAG Configuration
#    - Runs daily at 7:00 AM Eastern Time (change schedule_interval as needed).
# ------------------------------------------------------------------------

local_tz = pendulum.timezone("America/New_York")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, tzinfo=local_tz),
    'catchup': False
}

dag = DAG(
    dag_id='check_and_ingest_vulndb',
    default_args=default_args,
    schedule_interval='0 7 * * *',  # Runs daily at 7 AM ET
    description='Daily full load of golang/vulndb JSON files into Azure with overwrite.'
)

# ------------------------------------------------------------------------
# 2. Python Callable:
#    - List all JSON files in the golang/vulndb repo's 'data/osv/' folder.
#    - Download each file and upload to Azure Blob Storage (overwriting existing ones).
#    - Update the Airflow Variable with the complete list of ingested files.
# ------------------------------------------------------------------------

def check_and_ingest_vulndb(**kwargs):
    """
    Downloads and ingests all JSON files from the golang/vulndb repo's 'data/osv/' folder.
    Files are uploaded to Azure Blob Storage with overwrite enabled.
    """

    # 1) Use the GitHub API to list contents of the 'data/osv' directory in the golang/vulndb repo.
    github_api_url = (
        "https://api.github.com/repos/golang/vulndb/contents/data/osv?ref=master"
    )

    resp = requests.get(github_api_url)
    if resp.status_code != 200:
        raise ValueError(
            f"Failed to fetch content from GitHub API. Status code: {resp.status_code}, URL: {github_api_url}"
        )

    directory_content = resp.json()

    # 2) Filter for items that are "file" and end with ".json"
    json_files = [item for item in directory_content if item.get("type") == "file" and item["name"].endswith(".json")]

    if not json_files:
        print("No JSON files found in the data/osv/ directory.")
        return

    print(f"Found {len(json_files)} JSON file(s) to ingest:", [f["name"] for f in json_files])

    # 3) Download and upload each file to Azure Blob Storage
    wasb_hook = WasbHook(wasb_conn_id='azure_blob_conn')  # Must match your Airflow Connection ID

    ingested_files = []  # List to keep track of ingested file names

    for file_info in json_files:
        file_name = file_info["name"]
        download_url = file_info["download_url"]

        print(f"Downloading {file_name} from {download_url} ...")
        file_resp = requests.get(download_url)
        if file_resp.status_code != 200:
            print(f"WARNING: Could not download {file_name}. Skipping.")
            continue

        # Save the file locally (e.g., in /tmp)
        local_file_path = os.path.join("/tmp", file_name)
        with open(local_file_path, "wb") as f:
            f.write(file_resp.content)

        print(f"Uploading {file_name} to Azure Blob Storage...")
        wasb_hook.load_file(
            file_path=local_file_path,
            container_name="vulgo",  # Replace with your container name
            blob_name=file_name,         # Use the same name in the blob
            overwrite=True
        )
        print(f"Uploaded {file_name} successfully.")

        ingested_files.append(file_name)

        # Clean up the local file
        try:
            os.remove(local_file_path)
        except OSError:
            pass

    # 4) Update the Airflow Variable with the complete list of ingested files
    Variable.set("ingested_vulndb_files", json.dumps(ingested_files))
    print("Airflow Variable updated with the list of ingested files.")

# ------------------------------------------------------------------------
# 3. PythonOperator Task
# ------------------------------------------------------------------------

check_and_ingest_task = PythonOperator(
    task_id='check_and_ingest_vulndb_jsons',
    python_callable=check_and_ingest_vulndb,
    dag=dag
)
