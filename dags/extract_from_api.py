# /usr/local/airflow/dags/extract_from_api.py
import os
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from adlfs import AzureBlobFileSystem
from dotenv import load_dotenv
from include.api_functions import get_new_construction_data_from_internal_api
from airflow.datasets import Dataset
from datetime import datetime
import tempfile

# Load environment variables
load_dotenv()

AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME")

# -----------------------------
# Define Dataset for Airflow
# -----------------------------
INGEST_DATASET = Dataset(f"abfs://{AZURE_CONTAINER_NAME}/{INGEST_FOLDER_NAME}/")

# -----------------------------
# Helper function to upload CSV
# -----------------------------
def upload_csv_to_adls(local_file: str, remote_file: str) -> str:
    """
    Upload a local CSV file to Azure Data Lake and return the path
    """
    fs = AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT, account_key=AZURE_STORAGE_KEY)
    remote_path = f"{AZURE_CONTAINER_NAME}/{INGEST_FOLDER_NAME}/{remote_file}"
    print(f"Uploading {local_file} to abfs://{remote_path} ...")
    with fs.open(remote_path, "wb") as f:
        with open(local_file, "rb") as lf:
            f.write(lf.read())
    print(f"Uploaded {remote_file} successfully.")
    return f"abfs://{remote_path}"

# -----------------------------
# Define DAG
# -----------------------------
with DAG(
    dag_id="extract_from_api",
    description="Extract construction data from API and load to Azure Data Lake Gen2",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["construction", "adls", "etl"],
) as dag:

    @task(outlets=INGEST_DATASET)
    def get_new_construction_data():
        """
        Call internal API to generate new construction data and save to temporary CSVs
        Returns paths of CSVs in ADLS
        """
        num_activities = 10
        date_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

        # Get dataframes from API
        activities_df, projects_df, contractors_df, materials_df = get_new_construction_data_from_internal_api(
            num_activities=num_activities, date=pd.Timestamp.now()
        )

        # Save each DataFrame to a temp CSV, then upload
        csv_paths = {}
        for name, df in {
            "activities": activities_df,
            "projects": projects_df,
            "contractors": contractors_df,
            "materials": materials_df,
        }.items():
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp_file:
                df.to_csv(tmp_file.name, index=False)
                csv_paths[name] = upload_csv_to_adls(tmp_file.name, f"{name}_{date_str}.csv")

        return csv_paths  # XCom will now store paths instead of raw bytes

    @task
    def process_data(csv_paths: dict):
        """
        Dummy task to demonstrate usage of uploaded CSV paths
        """
        for name, path in csv_paths.items():
            print(f"{name} CSV uploaded to: {path}")

    # -----------------------------
    # Task dependencies
    # -----------------------------
    data_paths = get_new_construction_data()
    process_data(data_paths)