# /usr/local/airflow/dags/move_ingest_to_stage.py
import os
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset
from adlfs import AzureBlobFileSystem
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME")
STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME")

# Define dataset paths for lineage
INGEST_DATASET = Dataset(f"abfs://{AZURE_CONTAINER_NAME}/{INGEST_FOLDER_NAME}")
STAGE_DATASET = Dataset(f"abfs://{AZURE_CONTAINER_NAME}/{STAGE_FOLDER_NAME}")

# -----------------------------
# Helper function to move files
# -----------------------------
def move_files(fs: AzureBlobFileSystem, src_folder: str, dest_folder: str):
    """
    Move files from src_folder to dest_folder in ADLS
    """
    src_path = f"{AZURE_CONTAINER_NAME}/{src_folder}"
    dest_path = f"{AZURE_CONTAINER_NAME}/{dest_folder}"

    print(f"Moving files from abfs://{src_path} to abfs://{dest_path} ...")
    try:
        files = fs.ls(src_path)
        for file in files:
            filename = os.path.basename(file)
            src_file = f"{src_path}/{filename}"
            dest_file = f"{dest_path}/{filename}"
            print(f"Moving {src_file} -> {dest_file}")
            fs.mv(src_file, dest_file)
        print("Files moved successfully.")
    except Exception as e:
        print(f"Failed to move files: {e}")
        raise

# -----------------------------
# Define DAG
# -----------------------------
with DAG(
    dag_id="move_ingest_to_stage",
    description="Move files from Ingest to Stage folder in ADLS",
    schedule=[INGEST_DATASET],  # triggered by extract_from_api.py
    start_date=days_ago(1),
    catchup=False,
    tags=["construction", "adls", "etl"],
) as dag:

    @task(outlets=[STAGE_DATASET])  # publish STAGE_DATASET so move_stage_to_archive.py can be triggered
    def move_ingest_files_to_stage():
        """
        Move CSVs from Ingest folder to Stage folder
        """
        fs = AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT, account_key=AZURE_STORAGE_KEY)
        move_files(fs, INGEST_FOLDER_NAME, STAGE_FOLDER_NAME)

    move_ingest_files_to_stage()