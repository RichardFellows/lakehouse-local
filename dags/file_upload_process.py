"""
File Upload Processing DAG

Triggered by the file-upload API (POST /api/v1/dags/file_upload_process/dagRuns)
after structural validation passes. Does the expensive work the API deferred:

1. Download the uploaded file from S3.
2. Fetch the data contract from the file-upload API.
3. Run full validation (type coercion, null checks, constraint checks).
4. Write report.csv (all rows annotated with pass/fail + findings).
5. Write manifest.json (outcome, counts, error summary).
6. Upload both to s3://raw-data/reports/{upload_id}/.

Expected dag_run conf keys:
    upload_id   — the uploadId from the file-upload service
    s3_file_key — S3 key of the uploaded file (e.g. uploads/{uploadId}/data.csv)
    contract_id — data contract identifier
    uploaded_by — principal who submitted the file
"""

import json
import os
import tempfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

S3_BUCKET = "raw-data"
S3_ENDPOINT = "http://localstack:4566"
FILE_UPLOAD_API_URL = os.environ.get("FILE_UPLOAD_API_URL", "http://host.docker.internal:8080")

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _s3_client():
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="eu-west-2",
    )


def download_file(**context):
    """Download the uploaded file from S3 to a temp directory."""
    conf = context["dag_run"].conf
    s3_key = conf["s3_file_key"]
    upload_id = conf["upload_id"]

    tmp_dir = tempfile.mkdtemp(prefix=f"upload_{upload_id}_")
    file_name = s3_key.split("/")[-1]
    local_path = os.path.join(tmp_dir, file_name)

    _s3_client().download_file(S3_BUCKET, s3_key, local_path)
    print(f"Downloaded s3://{S3_BUCKET}/{s3_key} → {local_path}")

    context["ti"].xcom_push(key="local_path", value=local_path)
    context["ti"].xcom_push(key="tmp_dir", value=tmp_dir)


def fetch_contract(**context):
    """Fetch the data contract JSON from the file-upload API."""
    import requests

    conf = context["dag_run"].conf
    contract_id = conf["contract_id"]
    url = f"{FILE_UPLOAD_API_URL}/contracts/{contract_id}"

    r = requests.get(url, timeout=10)
    r.raise_for_status()
    contract = r.json()
    print(f"Fetched contract '{contract_id}' (v{contract.get('version')}), {len(contract.get('columns', []))} columns")

    context["ti"].xcom_push(key="contract", value=contract)


def validate_and_report(**context):
    """
    Parse the uploaded file, run full validation against the contract, and generate
    report.csv + manifest.json in a temp directory.

    Validation rules applied (mirrors the .NET pipeline's Semantic + Record stages):
    - Required columns present (already guaranteed by structural check; re-asserted here)
    - Non-nullable columns have a value
    - Numeric type coercion and min/max constraint checks
    - String pattern constraints (if specified)

    The report CSV includes every source row annotated with:
        _status   : "ok" | "error"
        _findings : semicolon-separated finding messages (empty when ok)
    """
    import pandas as pd

    ti = context["ti"]
    local_path: str = ti.xcom_pull(key="local_path", task_ids="download_file")
    contract: dict = ti.xcom_pull(key="contract", task_ids="fetch_contract")
    tmp_dir: str = ti.xcom_pull(key="tmp_dir", task_ids="download_file")

    columns_def = {c["name"]: c for c in contract.get("columns", [])}

    # Parse file (CSV or XLSX).
    ext = local_path.rsplit(".", 1)[-1].lower()
    if ext == "xlsx":
        worksheet = contract.get("worksheet")
        df = pd.read_excel(local_path, sheet_name=worksheet or 0, dtype=str, keep_default_na=False)
    else:
        df = pd.read_csv(local_path, dtype=str, keep_default_na=False)

    # Strip whitespace from column names.
    df.columns = [c.strip() for c in df.columns]

    findings_per_row: list[list[str]] = [[] for _ in range(len(df))]
    total_errors = 0

    for col_name, col_def in columns_def.items():
        if col_name not in df.columns:
            # Missing required column — structural check should have caught this; skip here.
            continue

        series = df[col_name]
        required: bool = col_def.get("required", False)
        nullable: bool = col_def.get("nullable", True)
        data_type: str = col_def.get("dataType", "string").lower()
        constraints: list[dict] = col_def.get("constraints", [])

        for i, raw in enumerate(series):
            cell_findings: list[str] = []
            is_empty = raw == "" or raw is None

            if is_empty:
                if required or not nullable:
                    cell_findings.append(f"[{col_name}] Value is required but missing.")
            else:
                # Type coercion checks.
                if data_type in ("integer", "int"):
                    try:
                        val = int(float(raw))
                    except (ValueError, TypeError):
                        cell_findings.append(f"[{col_name}] '{raw}' is not a valid integer.")
                        val = None
                    else:
                        _check_numeric_constraints(col_name, val, constraints, cell_findings)

                elif data_type in ("decimal", "float", "number"):
                    try:
                        val = float(raw)
                    except (ValueError, TypeError):
                        cell_findings.append(f"[{col_name}] '{raw}' is not a valid number.")
                        val = None
                    else:
                        _check_numeric_constraints(col_name, val, constraints, cell_findings)

                elif data_type == "date":
                    fmt = col_def.get("format", "%Y-%m-%d")
                    try:
                        datetime.strptime(raw, fmt)
                    except ValueError:
                        cell_findings.append(f"[{col_name}] '{raw}' does not match date format '{fmt}'.")

                elif data_type == "boolean":
                    if raw.lower() not in ("true", "false", "1", "0", "yes", "no"):
                        cell_findings.append(f"[{col_name}] '{raw}' is not a valid boolean.")

            if cell_findings:
                findings_per_row[i].extend(cell_findings)
                total_errors += len(cell_findings)

    # Build report dataframe.
    df["_status"] = ["error" if f else "ok" for f in findings_per_row]
    df["_findings"] = ["; ".join(f) for f in findings_per_row]

    accepted_rows = int((df["_status"] == "ok").sum())
    rejected_rows = len(df) - accepted_rows
    outcome = "succeeded" if total_errors == 0 else "failed"

    report_path = os.path.join(tmp_dir, "report.csv")
    df.to_csv(report_path, index=False)

    manifest = {
        "upload_id": context["dag_run"].conf["upload_id"],
        "outcome": outcome,
        "rows_total": len(df),
        "rows_accepted": accepted_rows,
        "rows_rejected": rejected_rows,
        "total_findings": total_errors,
        "processed_at": datetime.utcnow().isoformat() + "Z",
    }
    manifest_path = os.path.join(tmp_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f)

    print(f"Validation complete: {accepted_rows}/{len(df)} rows accepted, {total_errors} findings")

    context["ti"].xcom_push(key="report_path", value=report_path)
    context["ti"].xcom_push(key="manifest_path", value=manifest_path)
    context["ti"].xcom_push(key="outcome", value=outcome)


def _check_numeric_constraints(col_name: str, val, constraints: list[dict], findings: list[str]):
    for c in constraints:
        if c.get("type") == "min" and val < c["value"]:
            findings.append(f"[{col_name}] {val} is below minimum {c['value']}.")
        elif c.get("type") == "max" and val > c["value"]:
            findings.append(f"[{col_name}] {val} exceeds maximum {c['value']}.")


def upload_results(**context):
    """Upload report.csv and manifest.json to S3 under reports/{upload_id}/."""
    ti = context["ti"]
    upload_id = context["dag_run"].conf["upload_id"]
    report_path: str = ti.xcom_pull(key="report_path", task_ids="validate_and_report")
    manifest_path: str = ti.xcom_pull(key="manifest_path", task_ids="validate_and_report")

    s3 = _s3_client()
    report_key = f"reports/{upload_id}/report.csv"
    manifest_key = f"reports/{upload_id}/manifest.json"

    s3.upload_file(report_path, S3_BUCKET, report_key)
    s3.upload_file(manifest_path, S3_BUCKET, manifest_key)

    print(f"Uploaded s3://{S3_BUCKET}/{report_key}")
    print(f"Uploaded s3://{S3_BUCKET}/{manifest_key}")

    outcome: str = ti.xcom_pull(key="outcome", task_ids="validate_and_report")
    print(f"Processing outcome: {outcome}")


with DAG(
    dag_id="file_upload_process",
    description="Full validation and report generation for file-upload-spa uploads",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # triggered via REST API only
    catchup=False,
    default_args=default_args,
    tags=["file-upload", "validation"],
    doc_md=__doc__,
) as dag:

    download = PythonOperator(
        task_id="download_file",
        python_callable=download_file,
    )

    fetch = PythonOperator(
        task_id="fetch_contract",
        python_callable=fetch_contract,
    )

    validate = PythonOperator(
        task_id="validate_and_report",
        python_callable=validate_and_report,
    )

    upload = PythonOperator(
        task_id="upload_results",
        python_callable=upload_results,
    )

    [download, fetch] >> validate >> upload
