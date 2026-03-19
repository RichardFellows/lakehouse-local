"""
File Ingest DAG — Watch for CSV drops and load into Iceberg

Simulates a production SFTP-style feed:
1. Watches a local "landing zone" directory for new CSV files
2. Uploads them to S3 (raw-data bucket)
3. Loads into Iceberg tables via Spark SQL
4. Moves processed files to an archive folder
5. Triggers the dbt pipeline on the new data

Drop CSV files into /opt/feeds/incoming/ to trigger processing.
Files must follow the naming convention: {table_name}.csv or {table_name}_YYYYMMDD.csv

Example:
    cp products.csv /opt/feeds/incoming/
    # → creates/appends to nessie.raw.products table
    # → triggers dbt pipeline
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

FEEDS_DIR = "/opt/feeds/incoming"
ARCHIVE_DIR = "/opt/feeds/archive"
S3_BUCKET = "raw-data"
S3_PREFIX = "feeds"

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def check_for_files(**context):
    """Check landing zone for new CSV files. Branch based on result."""
    incoming = Path(FEEDS_DIR)
    incoming.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(incoming.glob("*.csv"))
    if not csv_files:
        return "no_files"

    file_list = [str(f) for f in csv_files]
    context["ti"].xcom_push(key="csv_files", value=file_list)
    print(f"Found {len(file_list)} file(s): {[f.name for f in csv_files]}")
    return "upload_to_s3"


def upload_to_s3(**context):
    """Upload CSVs to S3 raw-data bucket."""
    import subprocess

    files = context["ti"].xcom_pull(key="csv_files", task_ids="check_for_files")
    if not files:
        print("No files to upload")
        return

    uploaded = []
    for filepath in files:
        p = Path(filepath)
        # Derive table name: products.csv → products, products_20240601.csv → products
        table_name = p.stem.split("_")[0] if "_" in p.stem and p.stem.split("_")[-1].isdigit() else p.stem
        s3_key = f"{S3_PREFIX}/{table_name}/{p.name}"

        cmd = [
            "python3", "-c", f"""
import boto3
s3 = boto3.client('s3',
    endpoint_url='http://localstack:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='eu-west-2')
s3.upload_file('{filepath}', '{S3_BUCKET}', '{s3_key}')
print('Uploaded {p.name} → s3://{S3_BUCKET}/{s3_key}')
"""
        ]
        subprocess.run(cmd, check=True)
        uploaded.append({"file": filepath, "table_name": table_name, "s3_key": s3_key})
        print(f"✓ {p.name} → s3://{S3_BUCKET}/{s3_key}")

    context["ti"].xcom_push(key="uploaded_files", value=uploaded)


def load_to_iceberg(**context):
    """Load CSVs from S3 into Iceberg tables via Spark Thrift."""
    from pyhive import hive

    uploaded = context["ti"].xcom_pull(key="uploaded_files", task_ids="upload_to_s3")
    if not uploaded:
        print("No files to load")
        return

    conn = hive.connect(host="spark", port=10000, auth="NOSASL")
    cursor = conn.cursor()

    tables_loaded = []
    for item in uploaded:
        table_name = item["table_name"]
        s3_path = f"s3a://{S3_BUCKET}/{item['s3_key']}"

        print(f"Loading {s3_path} → raw.{table_name}")

        # Ensure raw namespace exists
        try:
            cursor.execute("CREATE NAMESPACE IF NOT EXISTS nessie.raw")
        except Exception:
            pass  # May already exist

        # Read CSV and create/append to Iceberg table
        # First, read schema from CSV to create table if needed
        cursor.execute(f"""
            CREATE OR REPLACE TEMPORARY VIEW _staging_{table_name}
            USING csv
            OPTIONS (
                path '{s3_path}',
                header 'true',
                inferSchema 'true'
            )
        """)

        # Check if table exists
        try:
            cursor.execute(f"DESCRIBE TABLE nessie.raw.{table_name}")
            table_exists = True
        except Exception:
            table_exists = False

        if table_exists:
            # Append to existing table
            cursor.execute(f"""
                INSERT INTO nessie.raw.{table_name}
                SELECT * FROM _staging_{table_name}
            """)
            print(f"  ✓ Appended to nessie.raw.{table_name}")
        else:
            # Create new Iceberg table from CSV
            cursor.execute(f"""
                CREATE TABLE nessie.raw.{table_name}
                USING iceberg
                AS SELECT * FROM _staging_{table_name}
            """)
            print(f"  ✓ Created nessie.raw.{table_name}")

        tables_loaded.append(table_name)

    conn.close()
    context["ti"].xcom_push(key="tables_loaded", value=tables_loaded)
    print(f"Loaded {len(tables_loaded)} table(s): {tables_loaded}")


def archive_files(**context):
    """Move processed files to archive directory."""
    import shutil

    files = context["ti"].xcom_pull(key="csv_files", task_ids="check_for_files")
    if not files:
        return

    archive = Path(ARCHIVE_DIR)
    archive.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    for filepath in files:
        p = Path(filepath)
        dest = archive / f"{p.stem}_{timestamp}{p.suffix}"
        shutil.move(str(p), str(dest))
        print(f"Archived: {p.name} → {dest.name}")


with DAG(
    dag_id="file_ingest",
    description="Watch for CSV files in landing zone, load into Iceberg via Spark",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",  # Check every 5 minutes
    catchup=False,
    default_args=default_args,
    tags=["lakehouse", "ingest", "iceberg", "feeds"],
    doc_md=__doc__,
) as dag:

    check = BranchPythonOperator(
        task_id="check_for_files",
        python_callable=check_for_files,
    )

    no_files = EmptyOperator(task_id="no_files")

    upload = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3,
    )

    load = PythonOperator(
        task_id="load_to_iceberg",
        python_callable=load_to_iceberg,
    )

    archive = PythonOperator(
        task_id="archive_files",
        python_callable=archive_files,
    )

    # Run dbt after ingest to rebuild marts with new data
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/dbt && DBT_TARGET=spark dbt run --profiles-dir /opt/dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/dbt && DBT_TARGET=spark dbt test --profiles-dir /opt/dbt",
    )

    check >> no_files
    check >> upload >> load >> archive >> dbt_run >> dbt_test
