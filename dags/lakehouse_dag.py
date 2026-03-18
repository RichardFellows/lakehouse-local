"""
Lakehouse DAG: Airflow → dbt → Spark (Iceberg/Nessie)

Demonstrates the full local lakehouse stack:
1. Seed reference data via dbt
2. Run staging models (views)
3. Build mart tables (Iceberg via Spark Thrift)
4. Run dbt tests
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_DIR = "/opt/dbt"
DBT_CMD = f"cd {DBT_DIR} && dbt"

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lakehouse_dbt_spark",
    description="dbt transformations on Spark/Iceberg/Nessie lakehouse",
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
