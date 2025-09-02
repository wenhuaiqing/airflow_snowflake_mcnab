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

# -----------------------------
# Dataset for triggering from load_to_snowflake
# -----------------------------
LOAD_COMPLETE_DATASET = Dataset(f"abfs://{AZURE_CONTAINER_NAME}/{ARCHIVE_FOLDER_NAME}/")

# -----------------------------
# Dataset for downstream tasks (so we don't loop)
# -----------------------------
ARCHIVE_DONE_DATASET = Dataset(f"abfs://{AZURE_CONTAINER_NAME}/{ARCHIVE_FOLDER_NAME}_done/")

# -----------------------------
# Logger
# -----------------------------
t_log = logging.getLogger("airflow.task")

# -----------------------------
# Helper function to move files
# -----------------------------
def move_files(fs: AzureBlobFileSystem, src_folder: str, dest_folder: str):
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
    schedule=[LOAD_COMPLETE_DATASET],  # triggered only by load_to_snowflake
    start_date=days_ago(1),
    catchup=False,
    tags=["construction", "adls", "etl"],
) as dag:

    @task(outlets=[ARCHIVE_DONE_DATASET])  # emit separate dataset to avoid loop
    def move_stage_files_to_archive():
        fs = AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT, account_key=AZURE_STORAGE_KEY)
        move_files(fs, STAGE_FOLDER_NAME, ARCHIVE_FOLDER_NAME)

    # @task
    # def verify_checksum(src_folder: str, dst_folder: str):
    #     fs = AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT, account_key=AZURE_STORAGE_KEY)
    #     src_files = fs.ls(f"{AZURE_CONTAINER_NAME}/{src_folder}")
    #     dst_files = fs.ls(f"{AZURE_CONTAINER_NAME}/{dst_folder}")

    #     src_checksums = get_all_checksums(path=f"{AZURE_CONTAINER_NAME}/{src_folder}", files=src_files)
    #     dst_checksums = get_all_checksums(path=f"{AZURE_CONTAINER_NAME}/{dst_folder}", files=dst_files)

    #     compare_checksums(
    #         src_checksums=src_checksums,
    #         dst_checksums=dst_checksums,
    #         folder_name_src=src_folder,
    #         folder_name_dst=dst_folder,
    #     )

    # @task
    # def del_all_files_from_stage():
    #     fs = AzureBlobFileSystem(account_name=AZURE_STORAGE_ACCOUNT, account_key=AZURE_STORAGE_KEY)
    #     files = fs.ls(f"{AZURE_CONTAINER_NAME}/{STAGE_FOLDER_NAME}")
    #     for f in files:
    #         fs.rm(f)
    #     t_log.info("Stage folder cleared.")

    # # -----------------------------
    # # Task dependencies
    # # -----------------------------
    moved_files = move_stage_files_to_archive()
    # verified = verify_checksum(STAGE_FOLDER_NAME, ARCHIVE_FOLDER_NAME)
    # cleared_stage = del_all_files_from_stage()

    # moved_files >> verified >> cleared_stage

    moved_files