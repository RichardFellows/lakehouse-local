"""
Lakehouse DAG — DuckDB Engine

Runs dbt transforms using DuckDB as the compute engine.
DuckDB runs in-process — no external services needed beyond S3 (LocalStack).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/dbt"
DBT_CMD = f"cd {DBT_DIR} && DBT_TARGET=duckdb dbt"

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lakehouse_duckdb",
    description="dbt transforms on DuckDB (lightweight, in-process)",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "dbt", "duckdb"],
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"{DBT_CMD} debug --profiles-dir {DBT_DIR}",
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_CMD} seed --profiles-dir {DBT_DIR}",
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"{DBT_CMD} run --select staging --profiles-dir {DBT_DIR}",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"{DBT_CMD} run --select marts --profiles-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_CMD} test --profiles-dir {DBT_DIR}",
    )

    dbt_debug >> dbt_seed >> dbt_run_staging >> dbt_run_marts >> dbt_test
