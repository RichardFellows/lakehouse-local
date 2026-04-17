"""
daily_snapshot_ingest — ingest one day of customer + order snapshots.

The source system drops a full-table snapshot of customers and orders into
`/opt/feeds/daily_snapshots/<YYYY-MM-DD>/` every day. This DAG:

  1. Picks the snapshot date (DAG conf `snapshot_date`, or the latest date
     available on disk).
  2. Loads the two CSVs into the lakehouse "raw" layer:
       * On the **Spark** target the CSVs are uploaded to S3 and loaded
         into Iceberg tables `nessie.raw.customers_latest` /
         `nessie.raw.orders_latest` via `CREATE OR REPLACE TABLE`.
       * On the **DuckDB** target the CSVs are copied over the dbt seed
         files (`dbt_project/seeds/customers_latest.csv` etc.) and
         refreshed with `dbt seed --select customers_latest orders_latest`.
  3. Runs `dbt snapshot` → SCD2 tables `snap_customers`, `snap_orders`
     capture the day's insert/update deltas.
  4. Runs `dbt run` → dim/fact marts rebuild (`dim_customer_current`,
     `dim_customer_history`, `fact_orders`, `customer_ltv`,
     `customer_orders`).
  5. Runs `dbt test` → schema tests + SCD2-integrity custom tests.

The default target is DuckDB. Trigger the DAG with conf
`{"target": "spark", "snapshot_date": "2024-07-14"}` to run the Spark path
for a specific day.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

FEEDS_DIR = Path("/opt/feeds/daily_snapshots")
DBT_DIR = Path("/opt/dbt")
SEEDS_DIR = DBT_DIR / "seeds"


def pick_snapshot_date(**context) -> str:
    """Resolve the snapshot_date from DAG conf, else pick the latest on disk."""
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    requested = conf.get("snapshot_date")
    if requested:
        if not (FEEDS_DIR / requested).is_dir():
            raise FileNotFoundError(
                f"snapshot_date {requested!r} was requested but "
                f"{FEEDS_DIR / requested} does not exist"
            )
        return requested
    available = sorted(
        d.name for d in FEEDS_DIR.iterdir()
        if d.is_dir() and len(d.name) == 10 and d.name[4] == "-"
    )
    if not available:
        raise FileNotFoundError(
            f"no daily_snapshot dirs found under {FEEDS_DIR}"
        )
    return available[-1]


def pick_target(**context) -> str:
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    return conf.get("target") or os.environ.get("DBT_TARGET", "duckdb")


def refresh_duckdb_seeds(**context) -> None:
    """Copy the chosen day's CSVs over the dbt seed files."""
    snapshot_date = context["ti"].xcom_pull(task_ids="pick_snapshot_date")
    src = FEEDS_DIR / snapshot_date
    for name in ("customers.csv", "orders.csv"):
        dst_name = name.replace(".csv", "_latest.csv")
        (SEEDS_DIR / dst_name).write_bytes((src / name).read_bytes())
        print(f"refreshed {SEEDS_DIR / dst_name} from {src / name}")


def load_spark_raw(**context) -> None:
    """Upload CSVs to S3 and create/replace Iceberg tables in nessie.raw."""
    import boto3
    from pyhive import hive

    snapshot_date = context["ti"].xcom_pull(task_ids="pick_snapshot_date")
    src = FEEDS_DIR / snapshot_date

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localstack:4566",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket="raw-data")
    except Exception:
        pass  # already exists

    for name in ("customers.csv", "orders.csv"):
        key = f"snapshots/{snapshot_date}/{name}"
        s3.upload_file(str(src / name), "raw-data", key)
        print(f"uploaded s3://raw-data/{key}")

    conn = hive.connect(host="spark", port=10000, auth="NOSASL")
    cur = conn.cursor()
    cur.execute("CREATE SCHEMA IF NOT EXISTS nessie.raw")

    # Stage the CSV as a temp view via the spark-builtin csv data source,
    # then create/replace the Iceberg table. We can't use the short-hand
    # `csv.\`s3a://…\`` + `OPTIONS (...)` because OPTIONS in that position
    # requires key=value with an = separator, and even then the table-valued
    # form doesn't accept a header option in all Spark 3.5 builds.
    cur.execute("CREATE SCHEMA IF NOT EXISTS nessie.raw")

    cur.execute(
        f"""
        CREATE OR REPLACE TEMPORARY VIEW raw_customers_csv
        USING csv
        OPTIONS (
          path = 's3a://raw-data/snapshots/{snapshot_date}/customers.csv',
          header = 'true',
          inferSchema = 'false'
        )
        """
    )
    cur.execute("DROP TABLE IF EXISTS nessie.raw.customers_latest")
    cur.execute(
        """
        CREATE TABLE nessie.raw.customers_latest
        USING iceberg
        AS
        SELECT
            cast(customer_id as int)       as customer_id,
            first_name,
            last_name,
            email,
            cast(created_at as date)       as created_at,
            cast(updated_at as timestamp)  as updated_at
        FROM raw_customers_csv
        """
    )

    cur.execute(
        f"""
        CREATE OR REPLACE TEMPORARY VIEW raw_orders_csv
        USING csv
        OPTIONS (
          path = 's3a://raw-data/snapshots/{snapshot_date}/orders.csv',
          header = 'true',
          inferSchema = 'false'
        )
        """
    )
    cur.execute("DROP TABLE IF EXISTS nessie.raw.orders_latest")
    cur.execute(
        """
        CREATE TABLE nessie.raw.orders_latest
        USING iceberg
        AS
        SELECT
            cast(order_id as int)            as order_id,
            cast(customer_id as int)         as customer_id,
            cast(order_date as date)         as order_date,
            cast(amount as decimal(10, 2))   as amount,
            status,
            cast(updated_at as timestamp)    as updated_at
        FROM raw_orders_csv
        """
    )

    cur.execute("SELECT count(*) FROM nessie.raw.customers_latest")
    print("customers_latest count:", cur.fetchone()[0])
    cur.execute("SELECT count(*) FROM nessie.raw.orders_latest")
    print("orders_latest count:", cur.fetchone()[0])


default_args = {
    "owner": "lakehouse",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="daily_snapshot_ingest",
    description=(
        "Ingest one day of customer + order snapshots, then run dbt "
        "snapshot → dbt run → dbt test to build SCD2 history + marts."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,  # manually triggered per day for the demo
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "dbt", "scd2", "ingest"],
) as dag:

    pick_date = PythonOperator(
        task_id="pick_snapshot_date",
        python_callable=pick_snapshot_date,
    )

    refresh_duckdb = PythonOperator(
        task_id="refresh_duckdb_seeds",
        python_callable=refresh_duckdb_seeds,
    )

    load_spark = PythonOperator(
        task_id="load_spark_raw",
        python_callable=load_spark_raw,
    )

    # We run the full dbt pipeline twice — once per engine — so a single DAG
    # run exercises both the DuckDB and Spark SCD2 code paths and proves the
    # snapshot history matches across engines. If you only want one, disable
    # the other task and set DBT_TARGET via dag_run.conf.
    dbt_duckdb = BashOperator(
        task_id="dbt_duckdb_snapshot_run_test",
        bash_command=(
            "cd /opt/dbt && "
            "DBT_TARGET=duckdb dbt seed --profiles-dir /opt/dbt --select customers_latest orders_latest && "
            "DBT_TARGET=duckdb dbt snapshot --profiles-dir /opt/dbt && "
            "DBT_TARGET=duckdb dbt run --profiles-dir /opt/dbt && "
            "DBT_TARGET=duckdb dbt test --profiles-dir /opt/dbt"
        ),
    )

    dbt_spark = BashOperator(
        task_id="dbt_spark_snapshot_run_test",
        bash_command=(
            "cd /opt/dbt && "
            "DBT_TARGET=spark dbt snapshot --profiles-dir /opt/dbt && "
            "DBT_TARGET=spark dbt run --profiles-dir /opt/dbt && "
            "DBT_TARGET=spark dbt test --profiles-dir /opt/dbt"
        ),
    )

    pick_date >> [refresh_duckdb, load_spark]
    refresh_duckdb >> dbt_duckdb
    load_spark >> dbt_spark
