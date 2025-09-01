"""
## Transform data in Snowflake to create construction analytics tables

This DAG transforms the construction data in Snowflake to create analytics tables. 

It creates the following tables:
- `ENRICHED_activities`
- `PROJECT_COST_ANALYSIS`
- `CONTRACTOR_PERFORMANCE`
- `MATERIAL_USAGE_ANALYSIS`
- `PROJECT_TIMELINE_ANALYSIS`
"""

import os
import logging
from pendulum import datetime, duration

from airflow.decorators import dag
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLColumnCheckOperator

_SNOWFLAKE_CONN_ID = os.getenv("SNOWFLAKE_CONN_ID", "snowflake_default")
_SNOWFLAKE_DB_NAME = os.getenv("SNOWFLAKE_DB_NAME", "ETL_DEMO")
_SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA_NAME", "DEV")

dag_directory = os.path.dirname(os.path.abspath(__file__))
t_log = logging.getLogger("airflow.task")

@dag(
    dag_display_name="Transform construction data in ❄️",
    start_date=datetime(2024, 8, 1),
    schedule=[Dataset(f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}")],
    catchup=False,
    max_consecutive_failed_dag_runs=10,
    default_args={
        "owner": "Data team",
        "retries": 3,
        "retry_delay": duration(minutes=1),
    },
    doc_md=__doc__,
    description="ETL",
    tags=["Snowflake", "Construction"],
    template_searchpath=[os.path.join(dag_directory, "../include/sql")],
)
def transform_data_in_snowflake():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # -----------------------------
    # ENRICHED_activities
    # -----------------------------
    create_enriched_activities = SQLExecuteQueryOperator(
        task_id="create_enriched_activities",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="create_enriched_activities.sql",
        params={"db_name": _SNOWFLAKE_DB_NAME, "schema_name": _SNOWFLAKE_SCHEMA_NAME},
    )

    upsert_enriched_activities = SQLExecuteQueryOperator(
        task_id="upsert_enriched_activities",
        conn_id=_SNOWFLAKE_CONN_ID,
        sql="upsert_enriched_activities.sql",
        params={"db_name": _SNOWFLAKE_DB_NAME, "schema_name": _SNOWFLAKE_SCHEMA_NAME},
    )

    vital_checks_enriched_activities_table = SQLColumnCheckOperator(
        task_id="vital_checks_enriched_activities_table",
        conn_id=_SNOWFLAKE_CONN_ID,
        database=_SNOWFLAKE_DB_NAME,
        table=f"{_SNOWFLAKE_SCHEMA_NAME}.enriched_activities",
        column_mapping={
            "ACTIVITY_ID": {"unique_check": {"equal_to": 0}, "null_check": {"equal_to": 0}}
        },
        outlets=[
            Dataset(f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.enriched_activities")
        ],
    )

    # -----------------------------
    # Create analytics tables after ENRICHED_activities
    # -----------------------------
    with TaskGroup(group_id="create_analytics_tables") as create_analytics_tables:

        create_project_cost_analysis = SQLExecuteQueryOperator(
            task_id="create_project_cost_analysis",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="create_project_cost_analysis.sql",
            params={"db_name": _SNOWFLAKE_DB_NAME, "schema_name": _SNOWFLAKE_SCHEMA_NAME},
        )

        create_contractor_performance = SQLExecuteQueryOperator(
            task_id="create_contractor_performance",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="create_contractor_performance.sql",
            params={"db_name": _SNOWFLAKE_DB_NAME, "schema_name": _SNOWFLAKE_SCHEMA_NAME},
        )

        create_material_usage_analysis = SQLExecuteQueryOperator(
            task_id="create_material_usage_analysis",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="create_material_usage_analysis.sql",
            params={"db_name": _SNOWFLAKE_DB_NAME, "schema_name": _SNOWFLAKE_SCHEMA_NAME},
        )

        create_project_timeline_analysis = SQLExecuteQueryOperator(
            task_id="create_project_timeline_analysis",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="create_project_timeline_analysis.sql",
            params={"db_name": _SNOWFLAKE_DB_NAME, "schema_name": _SNOWFLAKE_SCHEMA_NAME},
        )

    # -----------------------------
    # Upsert analytics tables after creation
    # -----------------------------
    with TaskGroup(group_id="upsert_analytics_tables") as upsert_analytics_tables:

        upsert_project_cost_analysis = SQLExecuteQueryOperator(
            task_id="upsert_project_cost_analysis",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="upsert_project_cost_analysis.sql",
            params={"db_name": _SNOWFLAKE_DB_NAME, "schema_name": _SNOWFLAKE_SCHEMA_NAME},
            outlets=[Dataset(f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.project_cost_analysis")],
        )

        upsert_contractor_performance = SQLExecuteQueryOperator(
            task_id="upsert_contractor_performance",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="upsert_contractor_performance.sql",
            params={"db_name": _SNOWFLAKE_DB_NAME, "schema_name": _SNOWFLAKE_SCHEMA_NAME},
            outlets=[Dataset(f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.contractor_performance")],
        )

        upsert_material_usage_analysis = SQLExecuteQueryOperator(
            task_id="upsert_material_usage_analysis",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="upsert_material_usage_analysis.sql",
            params={"db_name": _SNOWFLAKE_DB_NAME, "schema_name": _SNOWFLAKE_SCHEMA_NAME},
            outlets=[Dataset(f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.material_usage_analysis")],
        )

        upsert_project_timeline_analysis = SQLExecuteQueryOperator(
            task_id="upsert_project_timeline_analysis",
            conn_id=_SNOWFLAKE_CONN_ID,
            sql="upsert_project_timeline_analysis.sql",
            params={"db_name": _SNOWFLAKE_DB_NAME, "schema_name": _SNOWFLAKE_SCHEMA_NAME},
            outlets=[Dataset(f"snowflake://{_SNOWFLAKE_DB_NAME}.{_SNOWFLAKE_SCHEMA_NAME}.project_timeline_analysis")],
        )

    # -----------------------------
    # Define proper dependencies
    # -----------------------------
    chain(
        start,
        create_enriched_activities,
        upsert_enriched_activities,
        vital_checks_enriched_activities_table,
        create_analytics_tables,
        upsert_analytics_tables,
        end,
    )


transform_data_in_snowflake()