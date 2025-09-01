import os
from airflow.decorators import dag, task_group
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLColumnCheckOperator
from airflow.datasets import Dataset
from pendulum import datetime, duration
import logging

# -----------------------------
# Logging
# -----------------------------
t_log = logging.getLogger("airflow.task")

# -----------------------------
# Environment & Snowflake config
# -----------------------------
_AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "construction-data")
_STAGE_FOLDER_NAME = os.getenv("STAGE_FOLDER_NAME", "construction-stage")
_ARCHIVE_FOLDER_NAME = os.getenv("ARCHIVE_FOLDER_NAME", "construction-archive")
_SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME", "ETL_DEMO")
_SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "DEV")
_SNOWFLAKE_STAGE_NAME = os.getenv("SNOWFLAKE_STAGE_NAME", "CONSTRUCTION_STAGE")

LIST_OF_BASE_TABLE_NAMES = ["contractors", "materials", "projects", "activities"]

# -----------------------------
# DAG directory
# -----------------------------
dag_directory = os.path.dirname(os.path.abspath(__file__))
SQL_PATH = os.path.join(dag_directory, "../include/sql")

# -----------------------------
# Datasets for downstream DAGs
# -----------------------------
transform_dataset = Dataset(f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}")
archive_dataset = Dataset(f"abfs://{_AZURE_CONTAINER_NAME}/{_ARCHIVE_FOLDER_NAME}/")

# -----------------------------
# DAG definition
# -----------------------------
@dag(
    dag_id="load_to_snowflake",
    start_date=datetime(2024, 8, 1),
    schedule=[Dataset(f"abfs://{_AZURE_CONTAINER_NAME}/{_STAGE_FOLDER_NAME}/")],
    catchup=False,
    max_consecutive_failed_dag_runs=10,
    default_args={
        "owner": "Data team",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    template_searchpath=[SQL_PATH],
    description="Load construction data from ADLS to Snowflake",
    tags=["Azure", "Snowflake", "Construction"]
)
def load_to_snowflake():

    start = EmptyOperator(task_id="start")

    # Last task after all base tables are loaded
    base_tables_ready = EmptyOperator(
        task_id="base_tables_ready",
        outlets=[transform_dataset, archive_dataset],  # triggers downstream DAGs
    )

    for _TABLE in LIST_OF_BASE_TABLE_NAMES:

        @task_group(group_id=f"ingest_{_TABLE}_data")
        def ingest_data():
            create_table_if_not_exists = SQLExecuteQueryOperator(
                task_id=f"create_table_{_TABLE}_if_not_exists",
                conn_id=_SNOWFLAKE_CONN_ID,
                sql=f"create_{_TABLE}_table.sql",
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
                pattern=f".*{_TABLE}.*\\.csv",
                file_format="(type = 'CSV', field_delimiter = ',', skip_header = 1, field_optionally_enclosed_by='\"')",
            )

            deduplicate_records = SQLExecuteQueryOperator(
                task_id=f"deduplicate_records_{_TABLE}",
                conn_id=_SNOWFLAKE_CONN_ID,
                sql=f"remove_duplicate_{_TABLE}.sql",
                params={
                    "db_name": _SNOWFLAKE_DB_NAME,
                    "schema_name": _SNOWFLAKE_SCHEMA_NAME,
                },
            )

            # determine the primary key column name
            if _TABLE == "activities":
                pk_col = "activity_id"
            else:
                pk_col = f"{_TABLE[:-1]}_id"

            vital_dq_checks = SQLColumnCheckOperator(
                task_id=f"vital_checks_{_TABLE}_table",
                conn_id=_SNOWFLAKE_CONN_ID,
                database=_SNOWFLAKE_DB_NAME,
                table=f"{_SNOWFLAKE_SCHEMA_NAME}.{_TABLE}",
                column_mapping={
                    pk_col: {
                        "unique_check": {"equal_to": 0},
                        "null_check": {"equal_to": 0},
                    }
                },
                outlets=[
                    Dataset(f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.{_TABLE}")
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

    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    chain(base_tables_ready, end)

load_to_snowflake()