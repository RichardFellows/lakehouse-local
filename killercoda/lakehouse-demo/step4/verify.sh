#!/bin/bash
# Check the DuckDB database file was created by dbt
docker compose exec airflow test -f /opt/dbt/lakehouse.duckdb 2>/dev/null && exit 0 || exit 1
