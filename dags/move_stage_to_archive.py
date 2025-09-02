# /usr/local/airflow/dags/move_stage_to_archive.py
import os
import logging
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.datasets import Dataset
from adlfs import AzureBlobFileSystem
from dotenv import load_dotenv
from include.utils import get_all_files, get_all_checksums, compare_checksums

# -----------------------------
# Load environment variables
# -----------------------------
load_dotenv()

AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_STORAGE_KEY = os.getenv("AZURE_STORAGE_KEY")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME", "construction-stage")
ARCHIVE_FOLDER_NAME = os.getenv("ARCHIVE_FOLDER_NAME", "construction-archive")

# Define dataset paths for lineage
STAGE_DATASET = Dataset(f"abfs://{AZURE_CONTAINER_NAME}/{STAGE_FOLDER_NAME}")
ARCHIVE_DATASET = Dataset(f"abfs://{AZURE_CONTAINER_NAME}/{ARCHIVE_FOLDER_NAME}")

# Logger
t_log = logging.getLogger("airflow.task")

# -----------------------------
# Helper function to move files
# -----------------------------
def move_files(fs: AzureBlobFileSystem, src_folder: str, dest_folder: str):
    """
    Move files from src_folder to dest_folder in ADLS
    """
    src_path = f"{AZURE_CONTAINER_NAME}/{src_folder}"
    dest_path = f"{AZURE_CONTAINER_NAME}/{dest_folder}"

    t_log.info(f"Moving files from abfs://{src_path} to abfs://{dest_path} ...")
    try:
        files = fs.ls(src_path)
        for file in files:
            filename = os.path.basename(file)
            src_file = f"{src_path}/{filename}"
            dest_file = f"{dest_path}/{filename}"
            t_log.info(f"Moving {src_file} -> {dest_file}")
            fs.mv(src_file, dest_file)
        t_log.info("Files moved successfully.")
    except Exception as e:
        t_log.error(f"Failed to move files: {e}")
        raise

# -----------------------------
# DAG definition
# -----------------------------
with DAG(
    dag_id="move_stage_to_archive",
    description="Move files from Stage to Archive folder in ADLS",
    schedule=[STAGE_DATASET],  # triggered when STAGE_DATASET is updated
    start_date=days_ago(1),
    catchup=False,
    tags=["construction", "adls", "etl"],
) as dag:

    @task(outlets=[ARCHIVE_DATASET])
    def move_stage_files_to_archive():
        """
        Move files from Stage folder to Archive folder
        """
        fs = AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT, account_key=AZURE_STORAGE_KEY)
        move_files(fs, STAGE_FOLDER_NAME, ARCHIVE_FOLDER_NAME)

    @task
    def verify_checksum(src_folder: str, dst_folder: str):
        """
        Verify checksums between Stage and Archive folders
        """
        fs = AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT, account_key=AZURE_STORAGE_KEY)
        # List files
        src_files = fs.ls(f"{AZURE_CONTAINER_NAME}/{src_folder}")
        dst_files = fs.ls(f"{AZURE_CONTAINER_NAME}/{dst_folder}")

        # Compute checksums
        src_checksums = get_all_checksums(path=f"{AZURE_CONTAINER_NAME}/{src_folder}", files=src_files)
        dst_checksums = get_all_checksums(path=f"{AZURE_CONTAINER_NAME}/{dst_folder}", files=dst_files)

        # Compare
        compare_checksums(
            src_checksums=src_checksums,
            dst_checksums=dst_checksums,
            folder_name_src=src_folder,
            folder_name_dst=dst_folder,
        )

    @task
    def del_all_files_from_stage():
        """
        Delete all files in Stage folder after moving
        """
        fs = AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT, account_key=AZURE_STORAGE_KEY)
        files = fs.ls(f"{AZURE_CONTAINER_NAME}/{STAGE_FOLDER_NAME}")
        for f in files:
            fs.rm(f)
        t_log.info("Stage folder cleared.")

    # -----------------------------
    # DAG task dependencies
    # -----------------------------
    moved_files = move_stage_files_to_archive()
    verified = verify_checksum(STAGE_FOLDER_NAME, ARCHIVE_FOLDER_NAME)
    cleared_stage = del_all_files_from_stage()

    moved_files >> verified >> cleared_stage