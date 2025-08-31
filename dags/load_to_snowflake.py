"""
## Azure Data Lake Gen2 to Snowflake ETL DAG

This DAG extracts data CSV files stored in Azure Data Lake Gen2 and loads it 
into newly created Snowflake tables using a Snowflake Stage and the
CopyFromExternalStageToSnowflakeOperator.
The DAG parallelizes the loading of the CSV files into the Snowflake table.
Based on the folder structure in Azure Data Lake Gen2 to enable loading at scale.

To use this DAG, you need to have a Snowflake stage configured with a connection
to your Azure Data Lake Gen2 container.
"""

from airflow.decorators import dag, task_group, task
from airflow.datasets import Dataset
from airflow.models.baseoperator import chain
from airflow.io.path import ObjectStoragePath
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import (
    CopyFromExternalStageToSnowflakeOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.operators.sql import (
    SQLColumnCheckOperator,
    SQLTableCheckOperator,
)
from airflow.providers.slack.notifications.slack import SlackNotifier
from pendulum import datetime, duration
import logging
import os

## SET YOUR OWN CONTAINER NAME HERE
_AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "construction-data")
_STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME", "construction-stage")

# Get the Airflow task logger, print statements work as well to log at level INFO
t_log = logging.getLogger("airflow.task")

# Snowflake variables - REPLACE with your own values
_SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME", "ETL_DEMO")
_SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "DEV")
_SNOWFLAKE_STAGE_NAME = os.getenv("SNOWFLAKE_STAGE_NAME", "CONSTRUCTION_STAGE")

LIST_OF_BASE_TABLE_NAMES = ["contractors", "materials", "projects"]

# Creating ObjectStoragePath objects for Azure Data Lake Gen2
OBJECT_STORAGE_SRC = "abfs"
CONN_ID_SRC = os.getenv("AZURE_CONN_ID", "azure_default")
KEY_SRC = f"{_AZURE_CONTAINER_NAME}/{_STAGE_FOLDER_NAME}"

# Create the ObjectStoragePath object
base_src = ObjectStoragePath(f"{OBJECT_STORAGE_SRC}://{KEY_SRC}/", conn_id=CONN_ID_SRC)

dag_directory = os.path.dirname(os.path.abspath(__file__))

# -------------- #
# DAG definition #
# -------------- #

@dag(
    dag_display_name="Load construction data from Azure Data Lake Gen2 to ❄️",
    start_date=datetime(2024, 8, 1),
    schedule=[
        Dataset(base_src.as_uri())
    ],
    catchup=False,
    max_consecutive_failed_dag_runs=10,
    default_args={
        "owner": "Data team",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    doc_md=__doc__,
    description="ETL",
    tags=["Azure", "Snowflake", "Construction"],
    template_searchpath=[
        os.path.join(dag_directory, "../include/sql")
    ],
)
def load_to_snowflake():

    start = EmptyOperator(task_id="start")
    base_tables_ready = EmptyOperator(task_id="base_tables_ready")

    for _TABLE in LIST_OF_BASE_TABLE_NAMES:

        @task_group(group_id=f"ingest_{_TABLE}_data")
        def ingest_data():

            create_table_if_not_exists = SQLExecuteQueryOperator(
                task_id=f"create_table_{_TABLE}_if_not_exists",
                conn_id=_SNOWFLAKE_CONN_ID,
                sql=f"create_{_TABLE}_table.sql",
                show_return_value_in_logs=True,
                params={
                    "db_name": _SNOWFLAKE_DB_NAME,
                    "schema_name": _SNOWFLAKE_SCHEMA_NAME,
                },
            )

            copy_into_table = CopyFromExternalStageToSnowflakeOperator(
                task_id=f"copy_into_{_TABLE}_table",
                snowflake_conn_id=_SNOWFLAKE_CONN_ID,
                database=_SNOWFLAKE_DB_NAME,
                schema=_SNOWFLAKE_SCHEMA_NAME,
                table=_TABLE,
                stage=_SNOWFLAKE_STAGE_NAME,
                prefix=f"{_TABLE}/",
                file_format="(type = 'CSV', field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by = '\"')",
            )

            deduplicate_records = SQLExecuteQueryOperator(
                task_id=f"deduplicate_records_{_TABLE}",
                conn_id=_SNOWFLAKE_CONN_ID,
                sql=f"remove_duplicate_{_TABLE}.sql",
                show_return_value_in_logs=True,
                params={
                    "db_name": _SNOWFLAKE_DB_NAME,
                    "schema_name": _SNOWFLAKE_SCHEMA_NAME,
                },
            )

            vital_dq_checks = SQLColumnCheckOperator(
                task_id=f"vital_checks_{_TABLE}_table",
                conn_id=_SNOWFLAKE_CONN_ID,
                database=_SNOWFLAKE_DB_NAME,
                table=f"{_SNOWFLAKE_SCHEMA_NAME}.{_TABLE}",
                column_mapping={
                    f"{_TABLE[:-1]}_ID": {
                        "unique_check": {"equal_to": 0},
                        "null_check": {"equal_to": 0},
                    }
                },
                outlets=[
                    Dataset(
                        f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.{_TABLE}"
                    )
                ],
            )

            chain(
                start,
                create_table_if_not_exists,
                copy_into_table,
                deduplicate_records,
                vital_dq_checks,
                base_tables_ready,
            )

        ingest_data()

    @task_group(
        default_args={
            "owner": "DQ team",
            "retries": 0,
            "on_failure_callback": SlackNotifier(
                slack_conn_id="slack_conn",
                text="Data quality checks failed for the {{ task.table }} table!",
                channel="#alerts",
            ),
        },
    )
    def additional_dq_checks():

        from include.data_quality_checks import column_mappings

        SQLTableCheckOperator(
            task_id="additional_dq_checks_materials_table",
            conn_id=_SNOWFLAKE_CONN_ID,
            database=_SNOWFLAKE_DB_NAME,
            table=f"{_SNOWFLAKE_SCHEMA_NAME}.materials",
            checks={
                "material_category_check": {
                    "check_statement": "MATERIAL_CATEGORY IN ('Foundation', 'Structural', 'Framing', 'Interior', 'Roofing', 'Electrical', 'Plumbing', 'Insulation', 'Finishing', 'Masonry', 'Glazing', 'Exterior', 'Flooring', 'Tile', 'Countertops', 'Cabinetry', 'Fixtures', 'Lighting', 'HVAC', 'Windows', 'Doors', 'Stairs', 'Fencing', 'Landscaping', 'Sealants')"
                },
                "at_least_3_affordable_materials": {
                    "check_statement": "COUNT(DISTINCT MATERIAL_ID) >=3",
                    "partition_clause": "UNIT_PRICE < 100",
                },
            },
        )

        for _TABLE, _COLUMN_MAPPING in column_mappings.items():
            SQLColumnCheckOperator(
                task_id=f"additional_dq_checks_{_TABLE}_col",
                conn_id=_SNOWFLAKE_CONN_ID,
                database=_SNOWFLAKE_DB_NAME,
                table=f"{_SNOWFLAKE_SCHEMA_NAME}.{_TABLE}",
                column_mapping=_COLUMN_MAPPING,
            )

    @task_group
    def ingest_project_activities_data():

        create_table_project_activities_if_not_exists = SQLExecuteQueryOperator(
            task_id=f"create_table_project_activities_if_not_exists",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="create_project_activities_table.sql",
            show_return_value_in_logs=True,
            params={
                "db_name": _SNOWFLAKE_DB_NAME,
                "schema_name": _SNOWFLAKE_SCHEMA_NAME,
            },
        )

        copy_into_project_activities_table = CopyFromExternalStageToSnowflakeOperator(
            task_id="copy_into_project_activities_table",
            snowflake_conn_id=_SNOWFLAKE_CONN_ID,
            database=_SNOWFLAKE_DB_NAME,
            schema=_SNOWFLAKE_SCHEMA_NAME,
            table="project_activities",
            stage=_SNOWFLAKE_STAGE_NAME,
            prefix="project_activities/",
            file_format="(type = 'CSV', field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by = '\"')",
        )

        vital_dq_checks_project_activities_table = SQLColumnCheckOperator(
            task_id=f"vital_checks_project_activities_table",
            conn_id=_SNOWFLAKE_CONN_ID,
            database=_SNOWFLAKE_DB_NAME,
            table=f"{_SNOWFLAKE_SCHEMA_NAME}.project_activities",
            column_mapping={
                "ACTIVITY_ID": {
                    "unique_check": {"equal_to": 0},
                    "null_check": {"equal_to": 0},
                }
            },
            outlets=[
                Dataset(f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}")
            ],
        )

        chain(
            create_table_project_activities_if_not_exists,
            copy_into_project_activities_table,
            vital_dq_checks_project_activities_table,
        )

    ingest_project_activities_data_obj = ingest_project_activities_data()

    end = EmptyOperator(
        task_id="end",
        trigger_rule="all_done",
    )

    # ------------------- #
    # Define dependencies #
    # ------------------- #

    chain(
        base_tables_ready,
        ingest_project_activities_data_obj,
        additional_dq_checks(),
        end,
    )


load_to_snowflake()
