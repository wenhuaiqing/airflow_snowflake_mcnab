"""
## Extract data from an API and load it to Azure Data Lake Gen2

This DAG extracts construction data from a mock internal API and loads it 
into Azure Data Lake Gen2 using the Azure provider operators.
"""

from airflow.decorators import dag, task
from airflow.datasets import Dataset
from airflow.models.baseoperator import chain
from airflow.io.path import ObjectStoragePath
from airflow.models.param import Param
from pendulum import datetime, duration
import pandas as pd
import logging
import os

# Get the Airflow task logger
t_log = logging.getLogger("airflow.task")

# Azure Data Lake Gen2 variables
_AZURE_CONN_ID = os.getenv("AZURE_CONN_ID", "azure_default")
_AZURE_STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "my-storage-account")
_AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "construction-data")
_INGEST_FOLDER_NAME = os.getenv("INGEST_FOLDER_NAME", "construction-ingest")

# Creating ObjectStoragePath objects for Azure Data Lake Gen2
OBJECT_STORAGE_DST = "abfs"
CONN_ID_DST = _AZURE_CONN_ID
KEY_DST = _AZURE_CONTAINER_NAME + "/" + _INGEST_FOLDER_NAME

base_dst = ObjectStoragePath(f"{OBJECT_STORAGE_DST}://{KEY_DST}", conn_id=CONN_ID_DST)

# -------------- #
# DAG definition #
# -------------- #

@dag(
    dag_display_name="🏗️ Extract construction data from API and load to Azure Data Lake Gen2",
    start_date=datetime(2024, 8, 1),
    schedule="@daily",
    catchup=False,
    max_consecutive_failed_dag_runs=10,
    default_args={
        "owner": "Data team",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    params={
        "num_activities": Param(
            100,
            description="The number of construction activities to fetch from the API.",
            type="number",
        ),
    },
    doc_md=__doc__,
    description="ETL",
    tags=["ETL", "Azure", "Construction", "Internal API"],
)
def extract_from_api():
    
    @task
    def create_container():
        from airflow.providers.microsoft.azure.operators.adls_gen2 import ADLSGen2CreateContainerOperator
        
        container_creator = ADLSGen2CreateContainerOperator(
            task_id="create_container",
            azure_data_lake_conn_id=_AZURE_CONN_ID,
            container_name=_AZURE_CONTAINER_NAME,
        )
        return container_creator.execute({})

    # Create the Azure Data Lake Gen2 container if it does not exist yet
    create_container_task = create_container()
    
    @task
    def get_new_construction_data_from_api(**context) -> list[pd.DataFrame]:
        """
        Get new construction data from an internal API.
        Args:
            num_activities (int): The number of activities to fetch.
        Returns:
            list[pd.DataFrame]: A list of DataFrames containing construction data.
        """
        num_activities = context["params"]["num_activities"]
        date = context["ts"]
        from include.api_functions import get_new_construction_data_from_internal_api

        activities_df, projects_df, contractors_df, materials_df = get_new_construction_data_from_internal_api(
            num_activities, date
        )

        t_log.info(f"Fetching {num_activities} new construction activities from the internal API.")
        t_log.info(f"Head of the new activities data: {activities_df.head()}")
        t_log.info(f"Head of the new projects data: {projects_df.head()}")
        t_log.info(f"Head of the new contractors data: {contractors_df.head()}")
        t_log.info(f"Head of the new materials data: {materials_df.head()}")

        return [
            {"name": "project_activities", "data": activities_df},
            {"name": "projects", "data": projects_df},
            {"name": "contractors", "data": contractors_df},
            {"name": "materials", "data": materials_df},
        ]

    get_new_construction_data_from_api_obj = get_new_construction_data_from_api()

    @task(map_index_template="{{ my_custom_map_index }}")
    def write_to_azure_datalake(
        data_to_write: pd.DataFrame, base_dst: ObjectStoragePath, **context
    ):
        """
        Write the data to Azure Data Lake Gen2.
        Args:
            data_to_write (pd.DataFrame): The data to write to Azure.
            base_dst (ObjectStoragePath): The base path to write the data to.
        """
        import io

        data = data_to_write["data"]
        name = data_to_write["name"]
        dag_run_id = context["dag_run"].run_id

        csv_buffer = io.BytesIO()
        data.to_csv(csv_buffer, index=False)

        csv_bytes = csv_buffer.getvalue()

        path_dst = base_dst / name / f"{dag_run_id}.csv"

        # Write the bytes to Azure Data Lake Gen2
        path_dst.write_bytes(csv_bytes)

        # get the current context and define the custom map index variable
        from airflow.operators.python import get_current_context

        context = get_current_context()
        context["my_custom_map_index"] = f"Wrote new {name} data to Azure Data Lake Gen2."

    write_to_azure_datalake_obj = write_to_azure_datalake.partial(base_dst=base_dst).expand(
        data_to_write=get_new_construction_data_from_api_obj
    )

    @task(
        outlets=[
            Dataset(base_dst.as_uri()),
        ]
    )
    def confirm_ingest(base_path):
        """List files in Azure Data Lake Gen2."""
        path = base_path
        folders = [f for f in path.iterdir() if f.is_dir()]
        for folder in folders:
            t_log.info(f"Folder: {folder}")
            files = [f for f in folder.iterdir() if f.is_file()]
            for file in files:
                t_log.info(f"File: {file}")

    # ------------------------------ #
    # Define additional dependencies #
    # ------------------------------ #

    chain(
        create_container_task,
        get_new_construction_data_from_api_obj,
        write_to_azure_datalake_obj,
        confirm_ingest(base_path=base_dst),
    )

extract_from_api()
