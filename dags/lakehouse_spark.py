"""
Lakehouse DAG — Spark Engine

Runs dbt transforms using Spark Thrift Server as the compute engine.
Connects to Spark via PyHive, tables stored as Iceberg in Nessie catalog on S3.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/dbt"
DBT_CMD = f"cd {DBT_DIR} && DBT_TARGET=spark dbt"

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lakehouse_spark",
    description="dbt transforms on Spark/Iceberg/Nessie (production-parity)",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "dbt", "spark", "iceberg"],
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
